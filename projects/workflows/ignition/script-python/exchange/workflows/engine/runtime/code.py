# exchange/workflows/engine/runtime.py
"""
Workflows runtime facade and execution engine.

This module owns:
- workflow/step decorator registries for the current interpreter
- runtime helpers used inside workflow code (events, mailbox, cancellation checks)
- timer dispatch logic (in-memory queue flush + claim + dispatch)

_WORKFLOW_REGISTRY as a module-level Python dict because:
It holds Python callables, so it should stay interpreter-local.
It is not persisted in the Java kernel, which avoids interpreter-retention issues.
Decorator registration happens at import/load time, then mostly read-only.
Old interpreters (and their dicts) can still exist until drained,
so mixed-version behavior is controlled operationally by maintenance/swap.
Constraints:
- Jython 2.7 compatibility
- persistent state must stay in Java kernel objects (see engine.instance)
- no DB schema ownership here; this module calls the DB adapter only

Timestamp semantics:
- created_at_epoch_ms: enqueue time
- claimed_at_epoch_ms: claim ownership/lease time
- started_at_epoch_ms: actual user-code start time
- deadline_epoch_ms: started_at_epoch_ms + timeout (never claim time)
"""

import json
import time
import traceback

from java.util.concurrent import Executors, TimeUnit
from java.lang import Runnable, Thread

from exchange.workflows import settings
from exchange.workflows.util.date import nowMs
from exchange.workflows.engine.db import DB, uuid4
from exchange.workflows.is88 import models as isa88

log = system.util.getLogger(settings.getLoggerName("engine.runtime"))


def _safe(v):
    """
    Return a safe string value for logging fields.

    Args:
        v (object): Value to stringify for logs.

    Returns:
        str: String representation, or ``<unprintable>`` when conversion fails.
    """
    try:
        if v is None:
            return ""
        return str(v)
    except:
        return "<unprintable>"


def _threadName():
    """
    Best-effort current thread name for log context.

    Returns:
        str: Current thread name, or an empty string when unavailable.
    """
    try:
        return Thread.currentThread().getName()
    except:
        return ""


def _keyValueToString(kv):
    """
    Render key/value log fields in stable key order.

    Args:
        kv (dict): Log fields to format.

    Returns:
        str: Space-separated ``key=value`` string with stable key ordering.
    """
    # stable ordering is nice when grepping logs
    keys = kv.keys()
    keys.sort()
    parts = []
    for k in keys:
        parts.append("%s=%s" % (k, _safe(kv[k])))
    return " ".join(parts)


def _getSetSize(s):
    """
    Return a size value for Java/Python set-like objects.

    Args:
        s (object): Java/Python set-like object.

    Returns:
        int: Element count, or ``0`` when size cannot be read.
    """
    if s is None:
        return 0
    try:
        return int(s.size())
    except:
        try:
            return len(s)
        except:
            return 0


# ----------------------------
# Registries (import-time)
# ----------------------------
_WORKFLOW_REGISTRY = {}
_STEP_REGISTRY = {}

_WF_CONTROL_KEY = "__wf_control"
_WF_TIMEOUT_SECONDS_KEY = "timeout_seconds"
_DEFAULT_FAST_QUEUE_MAX_DEPTH = int(settings.FAST_QUEUE_MAX_SIZE)


def _coerceTimeoutSeconds(timeout_seconds):
    """
    Normalize timeout input to a positive float value or None.

    Args:
        timeout_seconds (float): Timeout in seconds for workflow execution.

    Returns:
        float|None: Positive timeout value, or ``None`` when unset/invalid.
    """
    if timeout_seconds is None:
        return None
    try:
        v = float(timeout_seconds)
        if v <= 0:
            return None
        return v
    except:
        return None


def _timeoutFromInputs(inputs_obj):
    """
    Read workflow timeout metadata from a decoded inputs payload.

    Args:
        inputs_obj (dict): Workflow input payload used internally.

    Returns:
        float|None: Timeout value parsed from internal control metadata.
    """
    if not isinstance(inputs_obj, dict):
        return None
    ctrl = inputs_obj.get(_WF_CONTROL_KEY)
    if not isinstance(ctrl, dict):
        return None
    return _coerceTimeoutSeconds(ctrl.get(_WF_TIMEOUT_SECONDS_KEY))


def _normalizeInputs(inputs_obj, timeout_seconds=None):
    """
    Normalize workflow inputs into a consistent internal envelope.

    The normalized payload always has:
    - `resolved`: value used as workflow function arguments
    - `raw`: original caller payload
    - optional `__wf_control.timeout_seconds`

    Args:
        inputs_obj (object): Caller-provided inputs payload.
        timeout_seconds (float|None): Optional timeout override.

    Returns:
        dict: Normalized payload for durable storage and runtime execution.
    """
    if isinstance(inputs_obj, dict):
        resolved = inputs_obj.get("resolved", inputs_obj)
        raw = inputs_obj.get("raw", inputs_obj)
    else:
        resolved = inputs_obj
        raw = inputs_obj

    payload = {"resolved": resolved, "raw": raw}

    timeout_s = _coerceTimeoutSeconds(timeout_seconds)
    if timeout_s is None:
        timeout_s = _timeoutFromInputs(
            inputs_obj if isinstance(inputs_obj, dict) else {}
        )
    if timeout_s is not None:
        payload[_WF_CONTROL_KEY] = {_WF_TIMEOUT_SECONDS_KEY: timeout_s}
    return payload


def workflow(name=None):
    """
    Decorator to register a workflow function.

    Args:
        name (str|None): Optional workflow registry name override.

    Returns:
        function: Decorator that registers and returns the workflow callable.
    """

    def _decorator(fn):
        """
        Register the decorated callable and return it.

        Args:
            fn (callable): Workflow function being decorated.

        Returns:
            callable: Same function after registration.
        """
        wf_name = name or fn.__name__
        _WORKFLOW_REGISTRY[wf_name] = fn
        return fn

    return _decorator


def step(name=None, options=None):
    """
    Decorator to register a step function with DBOS-style retry options.

    Args:
        name (str|None): Optional step registry name override.
        options (dict|None): Retry options (`retries_allowed`, `interval_seconds`,
            `max_attempts`, `backoff_rate`).

    Returns:
        function: Decorator that wraps step execution through runtime context.
    """

    def _decorator(fn):
        """
        Register the decorated callable and return it.

        Args:
            fn (callable): Step function being decorated.

        Returns:
            callable: Wrapped step callable registered in `_STEP_REGISTRY`.
        """
        stepName = name or fn.__name__
        opts = options or {}
        spec = {"name": stepName, "options": opts, "fn": fn}
        _STEP_REGISTRY[stepName] = spec

        def _wrapped(*args, **kwargs):
            """
            Route step calls through the active workflow runtime context.

            Args:
                *args (tuple): Positional arguments forwarded to step logic.
                **kwargs (dict): Keyword arguments forwarded to step logic.

            Returns:
                object: Step return value (or replayed cached value).
            """
            rt = WorkflowsRuntime.getCurrentRuntime()
            if rt is None:
                raise Exception(
                    "No active WorkflowsRuntime context. Steps must be called from a workflow."
                )
            return rt._runStep(stepName, fn, opts, args, kwargs)

        _wrapped.__name__ = fn.__name__
        _wrapped.__doc__ = fn.__doc__
        return _wrapped

    return _decorator


# ----------------------------
# Exceptions
# ----------------------------
class Cancelled(Exception):
    """Raised when cooperative cancellation is detected."""

    pass


class WorkflowNotFound(Exception):
    """Raised when a workflow name is not registered."""

    pass


# ----------------------------
# Thread-local context
# ----------------------------
class _Context(object):
    """Per-workflow execution context stored in thread local state."""

    def __init__(self, workflowId, workflowName, inputsObj, deadlineEpochMs=None):
        """
        Create workflow execution context for one workflow thread.

        Args:
            workflowId (str): Workflow id (UUID string).
            workflowName (str): Registered workflow name to execute.
            inputsObj (dict): Workflow input payload used internally.
            deadlineEpochMs (int|long|None): Absolute deadline used for cooperative timeout checks.

        Returns:
            None: Stores context fields on `self`.
        """
        self.workflowId = workflowId
        self.workflowName = workflowName
        self.inputs = inputsObj or {}
        self.deadlineEpochMs = deadlineEpochMs if deadlineEpochMs is not None else None
        self._callSeq = 0

    def nextCallSeq(self):
        """
        Advance and return the next deterministic step call sequence.

        Returns:
            int: Next deterministic step call sequence value.
        """
        self._callSeq += 1
        return self._callSeq


class _ThreadLocal(object):
    """Small wrapper around Java ThreadLocal for Jython ergonomics."""

    def __init__(self):
        """
        Create backing Java ThreadLocal storage.

        Returns:
            None: Initializes internal storage handle.
        """
        from java.lang import ThreadLocal

        self._tl = ThreadLocal()

    def get(self):
        """
        Read the current thread-local value.

        Returns:
            object: Value bound to the current thread, or `None`.
        """
        try:
            return self._tl.get()
        except:
            return None

    def set(self, v):
        """
        Set the current thread-local value.

        Args:
            v (object): Value to bind to the current thread.

        Returns:
            None: Updates thread-local storage.
        """
        try:
            self._tl.set(v)
        except:
            pass

    def clear(self):
        """
        Clear the current thread-local value.

        Returns:
            None: Clears thread-local storage for the current thread.
        """
        try:
            self._tl.remove()
        except:
            pass


# ----------------------------
# WorkflowsRuntime
# ----------------------------
class WorkflowsRuntime(object):
    """
    Main runtime facade around persistent Java kernel.

    Args:
            dbName (str): Ignition DB connection name used by the DB adapter.
            kernel (ConcurrentHashMap): Shared Java kernel persisted by engine.instance.
    """

    _runtimeThreadLocal = _ThreadLocal()
    _contextThreadLocal = _ThreadLocal()

    @classmethod
    def getCurrentRuntime(cls):
        """
        Return the runtime bound to the current thread.

        Returns:
            WorkflowsRuntime|None: Runtime bound to this thread, if any.
        """
        return cls._runtimeThreadLocal.get()

    @classmethod
    def getCurrentContext(cls):
        """
        Return the workflow context bound to the current thread.

        Returns:
            _Context|None: Workflow execution context bound to this thread.
        """
        return cls._contextThreadLocal.get()

    def __init__(self, dbName, kernel):
        """
        Create an ephemeral runtime facade around shared kernel components.
        Stores the data that needs to be concurrently accessed in a persistent java HashMap.

        Args:
            dbName (str): Ignition database connection name.
            kernel (ConcurrentHashMap): Shared Java kernel map.

        """
        self.dbName = dbName
        self.db = DB(dbName)
        self.kernel = kernel
        self.executor_id = str(kernel.get("executorId"))
        self._generation = kernel.get("generation")
        self._maintenanceEnabled = kernel.get("maintenanceEnabled")
        self._maintenanceMode = kernel.get("maintenanceMode")
        self._maintenanceReason = kernel.get("maintenanceReason")
        self._inFlightWorkflows = kernel.get("inFlightWorkflows")
        self._activePartitions = kernel.get("activePartitions")
        self._activeWorkflows = kernel.get("activeWorkflows")
        self._workflowExecutor = kernel.get("wfExecutor")
        self._stepExecutor = kernel.get("stepExecutor")
        self._inMemoryQueue = kernel.get("inMemoryQueue")
        self._inMemoryQueueMaxDepth = kernel.get("inMemoryQueueMaxDepth")
        self._inMemoryQueueOverflowCount = kernel.get("inMemoryQueueOverflowCount")
        self._inMemoryQueueLastOverflowAtMs = kernel.get(
            "inMemoryQueueLastOverflowAtMs"
        )
        self._inMemoryQueueOverflowMode = kernel.get("inMemoryQueueOverflowMode")
        self._inMemoryQueueAcceptDuringMaintenance = kernel.get(
            "inMemoryQueueAcceptDuringMaintenance"
        )
        self._inMemoryQueueFlushDuringMaintenance = kernel.get(
            "inMemoryQueueFlushDuringMaintenance"
        )
        self._workflowWorkers = kernel.get("workflowWorkers")
        self._stepWorkers = kernel.get("stepWorkers")
        self._lastSwapAtMs = kernel.get("lastSwapAtMs")
        self._shutdown = False

    def _setMaintenanceMode(self, mode):
        """
        Persist the maintenance mode in the shared kernel.
        This is needed since strings are immutable.

        Args:
            mode (str): Maintenance mode value (typically drain or cancel).

        Returns:
            None: Updates kernel-maintained maintenance mode.
        """
        try:
            self.kernel.put("maintenanceMode", str(mode))
            self._maintenanceMode = self.kernel.get("maintenanceMode")
        except:
            pass

    def _setMaintenanceReason(self, reason):
        """
        Persist a human-readable maintenance reason string.
        This is needed since strings are immutable.

        Args:
            reason (str): Human-readable reason for logs and status payloads.

        Returns:
            None: Updates kernel-maintained maintenance reason.
        """
        try:
            self.kernel.put("maintenanceReason", str(reason or ""))
            self._maintenanceReason = self.kernel.get("maintenanceReason")
        except:
            pass

    def _getInMemoryQueueOverflowMode(self):
        """
        Return normalized overflow mode for the in-memory queue.

        Returns:
            str: One of ``reject``, ``drop_newest``, or ``drop_oldest``.
        """
        try:
            mode = str(self._inMemoryQueueOverflowMode or "reject").strip().lower()
        except:
            mode = "reject"
        if mode not in ("reject", "drop_newest", "drop_oldest"):
            return "reject"
        return mode

    def _recordInMemoryQueueOverflow(self):
        """
        Record one in-memory queue overflow event.

        Returns:
            None: Best-effort counter/timestamp update.
        """
        try:
            self._inMemoryQueueOverflowCount.incrementAndGet()
            self._inMemoryQueueLastOverflowAtMs.set(nowMs())
        except:
            pass

    # ----------------------------
    # lifecycle
    # ----------------------------
    def shutdown(self, wait_sec=5):
        """
        Shut down workflow and step executors for this facade.

        Args:
            wait_sec (int): Seconds to wait during shutdown/await calls.

        Returns:
            None: Requests executor shutdown and waits up to `wait_sec`.
        """
        self._shutdown = True
        try:
            self._workflowExecutor.shutdown()
        except:
            pass
        try:
            self._stepExecutor.shutdown()
        except:
            pass
        try:
            self._workflowExecutor.awaitTermination(int(wait_sec), TimeUnit.SECONDS)
        except:
            pass
        try:
            self._stepExecutor.awaitTermination(int(wait_sec), TimeUnit.SECONDS)
        except:
            pass

    # ----------------------------
    # registry
    # ----------------------------
    def listWorkflows(self):
        """
        Return sorted registered workflow names.

        Returns:
            list[str]: Sorted workflow registry names.
        """
        return sorted(_WORKFLOW_REGISTRY.keys())

    def listSteps(self):
        """
        Return sorted registered step names.

        Returns:
            list[str]: Sorted step registry names.
        """
        return sorted(_STEP_REGISTRY.keys())

    def getWorkflowFn(self, workflowName):
        """
        Return a registered workflow function or raise when missing.

        Args:
            workflowName (str): Registered workflow name to execute.

        Returns:
            callable: Registered workflow function.
        """
        fn = _WORKFLOW_REGISTRY.get(workflowName)
        if fn is None:
            raise WorkflowNotFound("Workflow not registered: %s" % workflowName)
        return fn

    # ----------------------------
    # maintenance / admin
    # ----------------------------
    def getMaintenanceStatus(self):
        """
        Return a status snapshot for maintenance and kernel health.
        wfExecutorActive = the approximate number of threads (workers) that are actively executing tasks.

        Returns:
            dict: Maintenance and kernel health snapshot fields.
        """
        return {
            "maintenanceEnabled": self._maintenanceEnabled.get(),
            "maintenanceMode": self._maintenanceMode,
            "maintenanceReason": (
                str(self._maintenanceReason)
                if self._maintenanceReason is not None
                else ""
            ),
            "generation": self._generation.get(),
            "executorId": self.executor_id,
            "inFlightWorkflows": self._inFlightWorkflows.get(),
            "activeWorkflows": _getSetSize(self._activeWorkflows),
            "activePartitions": _getSetSize(self._activePartitions),
            "wfExecutorActive": self._workflowExecutor.getActiveCount(),
            "stepExecutorActive": self._stepExecutor.getActiveCount(),
            "inMemoryQueueDepth": self._inMemoryQueue.size(),
            "inMemoryQueueMaxDepth": self._inMemoryQueueMaxDepth.get(),
            "inMemoryQueueOverflowCount": self._inMemoryQueueOverflowCount.get(),
            "inMemoryQueueLastOverflowAtMs": self._inMemoryQueueLastOverflowAtMs.get(),
            "lastSwapAtMs": self._lastSwapAtMs.get(),
        }

    def enterMaintenance(self, mode="drain", reason="", queueName=None):
        """
        Enable maintenance mode and optionally cancel active work.

        Args:
            mode (str): `drain` (pause dispatch) or `cancel` (also cancel queued/running).
            reason (str): Operator reason persisted in status/logs.
            queueName (str|None): Optional queue scope for cancel operations.

        Returns:
            dict: Maintenance status snapshot, plus `ok` and `cancelled` counters.
        """
        mode = str(mode or "drain").lower()
        if mode not in ("drain", "cancel"):
            raise Exception("Invalid maintenance mode: %s" % mode)
        self._maintenanceEnabled.set(True)

        # Send message for maintenance banner in all perspective sessions to appear
        system.util.sendMessage(
            system.project.getProjectName(),
            "maintenanceStatus",
            payload={"status": True},
            scope="s",
        )
        self._setMaintenanceMode(mode)
        self._setMaintenanceReason(reason)

        cancelled = {"queued": 0, "running": 0}
        if mode == "cancel":
            tx = self.db.begin()
            try:
                cancelled["queued"] = int(
                    self.db.cancelEnqueuedPending(
                        queueName=queueName,
                        reason=reason or "maintenance_cancel",
                        tx=tx,
                    )
                )
                cancelled["running"] = int(
                    self.db.cancelRunning(
                        queueName=queueName,
                        reason=reason or "maintenance_cancel",
                        tx=tx,
                    )
                )
                self.db.commit(tx)
            except:
                self.db.rollback(tx)
                raise
            finally:
                self.db.close(tx)

        st = self.getMaintenanceStatus()
        st["ok"] = True
        st["cancelled"] = cancelled
        return st

    def exitMaintenance(self):
        """
        Disable maintenance mode and resume normal dispatch behavior.

        Returns:
            dict: Maintenance status snapshot after disable (`ok=True`).
        """
        self._maintenanceEnabled.set(False)
        # Send message for maintenance banner in all perspective sessions to disappear
        system.util.sendMessage(
            system.project.getProjectName(),
            "maintenanceStatus",
            payload={"status": False},
            scope="s",
        )
        self._setMaintenanceMode("drain")
        self._setMaintenanceReason("")
        st = self.getMaintenanceStatus()
        st["ok"] = True
        return st

    def swapIfDrained(self):
        """
        Swap executors only when in-flight and active work are fully drained.

        Returns:
            dict: Swap result with `ok`, `reason` (when blocked), and generation details.
        """
        if not self._maintenanceEnabled.get():
            return {
                "ok": False,
                "reason": "maintenance_required",
                "generation": self._generation.get(),
            }

        inFlight = self._inFlightWorkflows.get()
        activeWorkflowsCount = _getSetSize(self._activeWorkflows)
        activeWfExecutorCount = self._workflowExecutor.getActiveCount()
        activeStepCount = self._stepExecutor.getActiveCount()
        if (
            inFlight > 0
            or activeWorkflowsCount > 0
            or activeWfExecutorCount > 0
            or activeStepCount > 0
        ):
            # Cannot swap executors since there is at least one workflow running
            return {
                "ok": False,
                "reason": "not_drained",
                "inFlightWorkflows": inFlight,
                "activeWorkflows": activeWorkflowsCount,
                "wfExecutorActive": activeWfExecutorCount,
                "stepExecutorActive": activeStepCount,
                "generation": self._generation.get(),
            }

        prevWorkflowExec = self._workflowExecutor
        prevStepExec = self._stepExecutor
        workflowWorkers = self._workflowWorkers.get()
        stepWorkers = self._stepWorkers.get()
        newWorkflowExec = Executors.newFixedThreadPool(workflowWorkers)
        newStepExec = Executors.newFixedThreadPool(stepWorkers)
        self.kernel.put("wfExecutor", newWorkflowExec)
        self.kernel.put("stepExecutor", newStepExec)
        self._workflowExecutor = newWorkflowExec
        self._stepExecutor = newStepExec

        try:
            self._activePartitions.clear()
        except:
            pass
        try:
            self._activeWorkflows.clear()
        except:
            pass

        gen = self._generation.incrementAndGet()
        self._lastSwapAtMs.set(nowMs())

        try:
            prevWorkflowExec.shutdown()
        except:
            pass
        try:
            prevStepExec.shutdown()
        except:
            pass

        return {
            "ok": True,
            "generation": gen,
            "lastSwapAtMs": self._lastSwapAtMs.get(),
        }

    # ----------------------------
    # user-facing helpers (within workflow)
    # ----------------------------
    def setEvent(self, key, value):
        """
        Write a workflow-scoped event snapshot and history row.

        Args:
            key (str): Key name for event or stream operations.
            value (object): Value payload to persist or publish.

        Returns:
            None: Persists event snapshot + history rows in DB.
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            raise Exception("setEvent() must be called from within a workflow.")
        self.db.upsertEvent(ctx.workflowId, key, value, nowMs())

    def writeStream(self, key, value):
        """
        Append one value to a workflow stream key.

        Args:
            key (str): Key name for event or stream operations.
            value (object): Value payload to persist or publish.

        Returns:
            long: Appended stream offset.
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            raise Exception("writeStream() must be called from within a workflow.")
        return self.db.appendStream(ctx.workflowId, key, value, nowMs())

    def progress(self, message, extra=None):
        """
        Write a progress message to stream and phase event.

        Args:
            message (str|dict): Progress message to write.
            extra (dict|None): Optional structured context payload.

        Returns:
            None: Writes best-effort stream/event updates.
        """
        payload = {"ts": nowMs(), "message": message}
        if extra is not None:
            payload["extra"] = extra
        try:
            self.writeStream("log", payload)
        except:
            pass
        try:
            self.setEvent("phase.last_message", payload)
        except:
            pass

    def send(self, destinationWorkflowId, message, topic="cmd", messageUUID=None):
        """
        Send a mailbox notification to another workflow.

        Args:
            destinationWorkflowId (str): Destination workflow UUID.
            message (dict): Mailbox message payload.
            topic (str): Mailbox topic.
            messageUUID (str|None): Optional idempotency UUID.

        Returns:
            None: Inserts one notification row.
        """
        mu = messageUUID
        self.db.sendNotification(destinationWorkflowId, topic, message, mu, nowMs())

    def recv(self, topic="cmd", timeoutSeconds=0):
        """
        Receive one mailbox notification from the current workflow queue.

        Args:
            topic (str): Mailbox topic used for message routing.
            timeoutSeconds (float): Timeout in seconds for workflow execution.

        Returns:
            dict|None: Next mailbox message, or `None` on timeout/no message.
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            raise Exception("recv() must be called from within a workflow.")
        timeoutMs = long(float(timeoutSeconds) * 1000.0)
        return self.db.recvNotification(ctx.workflowId, topic, timeoutMs)

    def recvCommand(
        self, currentState=None, topic="cmd", timeoutSeconds=0, maxMessages=5
    ):
        """
        Receive mailbox commands and pick the highest-priority valid command.

        Priority and allowed-state checks are centralized in
        `exchange.workflows.is88.models` so workflow code does not duplicate
        STOP/HOLD/RESUME/RESET arbitration rules.

        Args:
            currentState (str|None): Current workflow state for command validation.
            topic (str): Mailbox topic used for message routing.
            timeoutSeconds (float): Wait time for the first message.
            maxMessages (int): Maximum messages to inspect for arbitration.

        Returns:
            dict|None: Chosen command payload, or `None` when mailbox is empty.
        """
        first = self.recv(topic=topic, timeoutSeconds=timeoutSeconds)
        if first is None:
            return None

        msgs = [first]
        want = int(maxMessages)
        if want < 1:
            want = 1
        while len(msgs) < want:
            msg = self.recv(topic=topic, timeoutSeconds=0)
            if msg is None:
                break
            msgs.append(msg)

        chosen = isa88.pickHighestPriority(msgs, state=currentState)
        return chosen

    def isCancelled(self):
        """
        Return True when the current workflow is marked CANCELLED.

        Returns:
            bool: True/False result for this operation.
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            return False
        row = self.db.getWorkflow(ctx.workflowId)
        if not row:
            return False
        return str(row.get("status")) == settings.STATUS_CANCELLED

    def checkCancelled(self):
        """
        Raise Cancelled when current workflow status is CANCELLED.

        Returns:
            None: Raises only when a cancellation flag is detected.
        """
        if self.isCancelled():
            raise Cancelled("Workflow was cancelled")

    def checkTimeout(self):
        """
        Raise Cancelled when the current workflow deadline is exceeded.

        Timeout is cooperative: the runtime marks DB status as CANCELLED and
        raises at step boundaries (or explicit workflow checks).

        Deadline is based on `started_at_epoch_ms + timeout` and does not begin
        at claim time.

        Returns:
            None: Raises `Cancelled` when timeout is exceeded.
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            return
        deadline_ms = getattr(ctx, "deadline_epoch_ms", None)
        if deadline_ms is None:
            row = self.db.getWorkflow(ctx.workflowId)
            if row:
                try:
                    deadline_ms = row.get("deadline_epoch_ms")
                except:
                    deadline_ms = None
                if deadline_ms is not None:
                    try:
                        ctx.deadlineEpochMs = deadline_ms
                    except:
                        pass
        if deadline_ms is None:
            return
        if nowMs() <= deadline_ms:
            return
        try:
            self.db.cancelWorkflow(ctx.workflowId, "Timeout exceeded")
        except:
            pass
        raise Cancelled("Workflow deadline exceeded")

    def _serializeResult(self, obj):
        """
        Serialize workflow result payload to compact JSON.

        Args:
            obj (object): Workflow return value.

        Returns:
            str|None: JSON text or `None`.
        """
        if obj is None:
            return None
        try:
            return json.dumps(obj, separators=(",", ":"))
        except Exception as e:
            try:
                return json.dumps(
                    {
                        "type": "result_serialize_error",
                        "message": str(e),
                        "value": _safe(obj),
                    },
                    separators=(",", ":"),
                )
            except:
                return json.dumps(
                    {"type": "result_serialize_error"}, separators=(",", ":")
                )

    def _serializeError(self, exc, errType="workflow_error", extra=None):
        """
        Build a JSON-safe error payload.

        Args:
            exc (Exception): Exception instance.
            errType (str): Stable error type label.
            extra (dict|None): Optional additional structured fields.

        Returns:
            dict: Error payload safe for DB JSON serialization.
        """
        payload = {
            "type": str(errType or "error"),
            "message": _safe(exc),
            "traceback": traceback.format_exc(),
        }
        if isinstance(extra, dict):
            payload.update(extra)
        return payload

    def _recordWorkflowTerminal(self, workflowId, status, fields=None):
        """
        Persist terminal workflow status with terminal status.

        Args:
            workflowId (str): Workflow UUID.
            status (str): Terminal status (`SUCCESS`, `ERROR`, `CANCELLED`).
            fields (dict|None): Additional fields to persist.

        Returns:
            int: Number of updated rows.
        """
        fields = fields or {}
        return self.db.updateWorkflowStatusIfNotTerminal(
            workflowId, status, fields=fields
        )

    def _executeWithRetry(
        self,
        fn,
        attempts,
        intervalS,
        backoffRate,
        wid,
        wfName,
        stepName,
        callSeq,
        args,
        kwargs,
        maxIntervalS=3600.0,
    ):
        """
        Execute callable with DBOS-style retry/backoff behavior.

        Args:
            fn (callable): Step function.
            attempts (int): Maximum attempts to run.
            intervalS (float): Initial retry delay in seconds.
            backoffRate (float): Backoff multiplier per retry.
            wid (str): Workflow UUID for logs.
            wfName (str): Workflow name for logs.
            stepName (str): Step name for logs.
            callSeq (int): Deterministic step call sequence.
            args (tuple): Positional step args.
            kwargs (dict): Keyword step args.
            maxIntervalS (float): Retry delay cap.

        Returns:
            tuple: `(result, attemptsUsed)`.

        Raises:
            Cancelled: When cancellation/timeout is detected.
            Exception: Last step exception after retries are exhausted.
        """
        attempts = int(attempts or 1)
        if attempts < 1:
            attempts = 1
        delay_s = float(intervalS or 0.0)
        if delay_s < 0:
            delay_s = 0.0
        backoff = float(backoffRate or 1.0)
        if backoff <= 0:
            backoff = 1.0
        max_delay_s = float(maxIntervalS or 3600.0)
        if max_delay_s <= 0:
            max_delay_s = 3600.0

        attempt = 0
        last_exc = None
        while attempt < attempts:
            attempt += 1
            try:
                self.checkCancelled()
                self.checkTimeout()
                return fn(*args, **kwargs), attempt
            except Cancelled:
                raise
            except Exception as e:
                last_exc = e
                has_next = attempt < attempts
                next_delay_s = delay_s if has_next else 0.0
                level = "warn" if has_next else "error"
                self._logEvent(
                    level,
                    "step.error",
                    wid=wid,
                    wf=wfName,
                    step=stepName,
                    call_seq=callSeq,
                    attempt=attempt,
                    next_delay_s=next_delay_s,
                    msg=str(e),
                )
                err_payload = self._serializeError(
                    e,
                    errType="step_error",
                    extra={
                        "step": stepName,
                        "call_seq": callSeq,
                        "attempt": attempt,
                    },
                )
                try:
                    self.db.updateStepError(wid, callSeq, err_payload, nowMs(), attempt)
                except:
                    pass
                if not has_next:
                    raise
                if next_delay_s > 0:
                    time.sleep(next_delay_s)
                delay_s = min(max_delay_s, next_delay_s * backoff)

        if last_exc is not None:
            raise last_exc
        raise Exception("Step failed: %s" % stepName)

    # ----------------------------
    # enqueue / start
    # ----------------------------
    def enqueue(
        self,
        workflowName,
        inputsObj=None,
        queueName=None,
        partitionKey=None,
        priority=0,
        deduplicationId=None,
        applicationVersion=None,
        timeoutSeconds=None,
        allowDuringMaintenance=False,
    ):
        """
        Validate and enqueue a workflow row in durable storage.

        Args:
            workflowName (str): Registered workflow name to execute.
            inputsObj (dict): Workflow input payload used internally.
            queueName (str|None): Queue name used for enqueue/claim routing.
            partitionKey (str): Partition key used for resource serialization.
            priority (int): Priority value used when ordering queued workflows.
            deduplicationId (str): Optional deduplication key for idempotent enqueue.
            applicationVersion (str): Optional application/version stamp persisted with the run.
            timeoutSeconds (float): Timeout in seconds for workflow execution.
            allowDuringMaintenance (bool): When True, bypasses maintenance rejection checks.

        Returns:
            str: New workflow UUID.
        """
        queueName = settings.resolveQueueName(queueName)
        if self._maintenanceEnabled.get() and not bool(allowDuringMaintenance):
            raise Exception("Maintenance enabled: start rejected")

        workflowId = uuid4()
        created = nowMs()
        self.getWorkflowFn(workflowName)  # fail fast if not registered

        timeout_s = _coerceTimeoutSeconds(timeoutSeconds)
        inputsObj = _normalizeInputs(inputsObj, timeout_s)

        # DBOS-style timeout starts when execution begins, not while queued.
        deadline_ms = None

        tx = self.db.begin()
        try:
            self.db.insertWorkflow(
                workflowId=workflowId,
                workflowName=workflowName,
                queueName=queueName,
                partitionKey=partitionKey,
                priority=priority,
                deduplicationId=deduplicationId,
                applicationVersion=applicationVersion,
                inputsObj=inputsObj,
                createdMs=created,
                deadlineMs=deadline_ms,
                tx=tx,
            )
            self.db.commit(tx)
        except:
            self.db.rollback(tx)
            raise
        finally:
            self.db.close(tx)

        self._logEvent(
            "info",
            "wf.enqueue",
            wid=workflowId,
            wf=workflowName,
            queue=queueName,
            pkey=partitionKey,
            prio=priority,
            dedupe=deduplicationId,
            timeout_s=timeout_s,
            deadline_ms=deadline_ms,
        )
        return workflowId

    def enqueueInMemory(
        self,
        workflowName,
        inputsObj=None,
        queueName=None,
        partitionKey=None,
        priority=0,
        deduplicationId=None,
        applicationVersion=None,
        timeoutSeconds=None,
        allowDuringMaintenance=None,
    ):
        """
        Push a lightweight enqueue request onto the in-memory queue.

        Args:
            workflowName (str): Registered workflow name to execute.
            inputsObj (dict): Workflow input payload used internally.
            queueName (str): Queue name used for enqueue/claim routing.
            partitionKey (str): Partition key used for resource serialization.
            priority (int): Priority value used when ordering queued workflows.
            deduplicationId (str): Optional deduplication key for idempotent enqueue.
            applicationVersion (str): Optional application/version stamp persisted with the run.
            timeoutSeconds (float): Timeout in seconds for workflow execution.
            allowDuringMaintenance (bool): When True, bypasses maintenance rejection checks.

        Returns:
            dict: Ack payload with `accepted`, queue depth, and optional rejection reason.
        """
        queueName = settings.resolveQueueName(queueName)
        if allowDuringMaintenance is None:
            allowDuringMaintenance = self._inMemoryQueueAcceptDuringMaintenance.get()
        if self._maintenanceEnabled.get() and not bool(allowDuringMaintenance):
            return {
                "accepted": False,
                "reason": "maintenance",
                "queueDepth": self._inMemoryQueue.size(),
            }

        timeout_s = _coerceTimeoutSeconds(timeoutSeconds)
        payload = {
            "workflow_name": workflowName,
            "inputs": inputsObj if inputsObj is not None else {},
            "queue_name": queueName,
            "partition_key": partitionKey,
            "priority": int(priority),
            "deduplication_id": deduplicationId,
            "application_version": applicationVersion,
            "timeout_seconds": timeout_s,
            "accepted_at_ms": nowMs(),
        }
        try:
            raw = json.dumps(payload, separators=(",", ":"))
        except:
            return {
                "accepted": False,
                "reason": "invalid_payload",
                "queueDepth": self._inMemoryQueue.size(),
            }

        current_depth = self._inMemoryQueue.size()
        max_depth = self._inMemoryQueueMaxDepth.get()
        if max_depth > 0 and current_depth >= max_depth:
            mode = self._getInMemoryQueueOverflowMode()
            if mode == "drop_oldest":
                dropped = self._inMemoryQueue.poll()
                if dropped is None:
                    self._recordInMemoryQueueOverflow()
                    self._logEvent(
                        "warn",
                        "in_memory.enqueue.overflow",
                        depth=current_depth,
                        max_depth=max_depth,
                        mode=mode,
                        reason="drop_oldest_empty",
                    )
                    return {
                        "accepted": False,
                        "reason": "overflow",
                        "queueDepth": current_depth,
                        "maxDepth": max_depth,
                        "mode": mode,
                    }
                self._inMemoryQueue.offer(raw)
                new_depth = self._inMemoryQueue.size()
                self._logEvent(
                    "warn",
                    "in_memory.enqueue.overflow.drop_oldest",
                    depth=current_depth,
                    max_depth=max_depth,
                    mode=mode,
                    queue_depth=new_depth,
                )
                return {
                    "accepted": True,
                    "queueDepth": new_depth,
                    "droppedOldest": True,
                    "mode": mode,
                }

            # drop_newest and reject both reject the incoming item.
            self._recordInMemoryQueueOverflow()
            self._logEvent(
                "warn",
                "in_memory.enqueue.overflow",
                depth=current_depth,
                max_depth=max_depth,
                mode=mode,
            )
            return {
                "accepted": False,
                "reason": "overflow",
                "queueDepth": current_depth,
                "maxDepth": max_depth,
                "mode": mode,
            }

        self._inMemoryQueue.offer(raw)
        return {"accepted": True, "queueDepth": self._inMemoryQueue.size()}

    def flushInMemoryQueue(
        self, maxItems=None, maxMs=None, allowDuringMaintenance=None
    ):
        """
        Flush bounded in-memory queue items into regular workflow rows.

        Args:
            maxItems (int): Maximum number of items to process in one call.
            maxMs (int): Maximum elapsed milliseconds allowed for one call.
            allowDuringMaintenance (bool): When True, bypasses maintenance rejection checks.

        Returns:
            int: Number of rows inserted into `workflows.workflow_status`.
        """
        if maxItems is None:
            maxItems = int(settings.FAST_FLUSH_MAX_ITEMS)
        if maxMs is None:
            maxMs = int(settings.FAST_FLUSH_MAX_MS)
        if allowDuringMaintenance is None:
            allowDuringMaintenance = self._inMemoryQueueFlushDuringMaintenance.get()
        if self._maintenanceEnabled.get() and not bool(allowDuringMaintenance):
            return 0

        started = nowMs()
        to_insert = []
        raw_items = []
        while len(raw_items) < int(maxItems):
            if (nowMs() - started) >= maxMs:
                break
            raw = self._inMemoryQueue.poll()
            if raw is None:
                break
            raw_items.append(raw)
            try:
                item = json.loads(raw)
            except:
                log.warn("inMemoryQueue payload decode failed")
                continue
            wf_name = item.get("workflow_name")
            if not wf_name:
                log.warn("inMemoryQueue payload missing workflow_name")
                continue
            to_insert.append(item)

        if not to_insert:
            return 0

        created_ms = nowMs()
        rows = []
        for item in to_insert:
            timeout_s = _coerceTimeoutSeconds(item.get("timeout_seconds"))
            inputs_obj = _normalizeInputs(item.get("inputs"), timeout_s)
            rows.append(
                {
                    "workflow_id": uuid4(),
                    "workflow_name": item.get("workflow_name"),
                    "queue_name": item.get("queue_name") or settings.QUEUE_DEFAULT,
                    "partition_key": item.get("partition_key"),
                    "priority": int(item.get("priority") or 0),
                    "deduplication_id": item.get("deduplication_id"),
                    "application_version": item.get("application_version"),
                    "inputs_obj": inputs_obj,
                    "created_ms": created_ms,
                    "deadline_ms": None,
                }
            )

        tx = self.db.begin()
        inserted = 0
        try:
            inserted = int(self.db.insertWorkflowsBatch(rows, tx=tx))
            self.db.commit(tx)
        except Exception as e:
            self.db.rollback(tx)
            # best effort requeue on flush failure
            for raw in raw_items:
                try:
                    self._inMemoryQueue.offer(raw)
                except:
                    pass
            log.error("inMemoryQueue flush failed: %s" % e)
        finally:
            self.db.close(tx)
        return inserted

    # ----------------------------
    # claim / dispatch
    # ----------------------------
    def dispatch(
        self,
        queueName=None,
        maxToClaim=None,
        flushMaxItems=None,
        flushMaxMs=None,
    ):
        """
        Flush in-memory queue, claim ENQUEUED workflows, and dispatch them to worker threads.

        Computes an effective claim limit based on real executor capacity:
        `available = workflowWorkers - (active + queued)`.
        It only claims up to `min(maxToClaim, available)` so rows are not over-claimed
        into PENDING when the local executor cannot run them promptly.

        Args:
            queueName (str|None): Queue name used for enqueue/claim routing.
            maxToClaim (int|None): Maximum workflows to claim in one dispatch run.
            flushMaxItems (int|None): In-memory queue items to flush before claim.
            flushMaxMs (int|None): Flush time budget in milliseconds.

        Returns:
            int: Number of workflow rows claimed for dispatch.
        """
        queueName = settings.resolveQueueName(queueName)
        if maxToClaim is None:
            maxToClaim = int(settings.MAX_CLAIM_DEFAULT)
        if flushMaxItems is None:
            flushMaxItems = int(settings.FAST_FLUSH_MAX_ITEMS)
        if flushMaxMs is None:
            flushMaxMs = int(settings.FAST_FLUSH_MAX_MS)
        self._logEvent(
            "trace",
            "dispatch.begin",
            queue=queueName,
            max=maxToClaim,
            active_wf=_getSetSize(self._activeWorkflows),
            active_pkey=_getSetSize(self._activePartitions),
        )
        if self._shutdown:
            return 0

        try:
            flushed = self.flushInMemoryQueue(maxItems=flushMaxItems, maxMs=flushMaxMs)
            if flushed > 0:
                self._logEvent(
                    "debug", "in_memory.flush", queue=queueName, inserted=flushed
                )
        except Exception as e:
            log.warn("flushInMemoryQueue failed: %s" % e)

        if self._maintenanceEnabled.get():
            self._logEvent(
                "info",
                "dispatch.maintenance.skip_dispatch",
                mode=self._maintenanceMode,
            )
            return 0

        try:
            self._cancelExpiredRuns()
        except Exception as e:
            log.warn("_cancelExpiredRuns failed: %s" % e)

        workers = 0
        try:
            workers = self._workflowWorkers.get()
        except:
            workers = 0
        if workers < 0:
            workers = 0
        active = self._workflowExecutor.getActiveCount()
        queued = self._workflowExecutor.getQueue().size()
        available = workers - (active + queued)
        if available < 0:
            available = 0
        want = int(maxToClaim)
        if want < 0:
            want = 0
        if available < want:
            want = available
        if want <= 0:
            self._logEvent(
                "debug",
                "dispatch.no_capacity",
                queue=queueName,
                workers=workers,
                active=active,
                queued=queued,
                requested=maxToClaim,
            )
            return 0

        tx = self.db.begin()
        claimed = []
        try:
            claimed = self.db.claimEnqueued(queueName, want, self.executor_id, tx=tx)
            self.db.commit(tx)
        except Exception as e:
            self.db.rollback(tx)
            log.error("Claim failed: %s" % e)
            return 0
        finally:
            self.db.close(tx)

        if self._maintenanceEnabled.get():
            released = 0
            for row in claimed:
                wid = str(row.get("workflow_id"))
                try:
                    released += int(self.db.releaseClaim(wid))
                except Exception as e:
                    self._logEvent(
                        "error",
                        "dispatch.maintenance.releaseClaim_error",
                        wid=wid,
                        msg=str(e),
                    )
            self._logEvent(
                "info",
                "dispatch.maintenance.releaseClaims",
                queue=queueName,
                claimed=len(claimed),
                released=released,
            )
            return 0

        for row in claimed:
            wid = str(row.get("workflow_id"))
            pkey = row.get("partition_key")
            if pkey is not None:
                pkey = str(pkey)

            if pkey is not None:
                try:
                    if self._activePartitions.contains(pkey):
                        released = int(self.db.releaseClaim(wid))
                        if released != 1:
                            self._logEvent(
                                "error",
                                "dispatch.skip.partition_busy.release_failed",
                                wid=wid,
                                wf=str(row.get("workflow_name")),
                                pkey=pkey,
                            )
                            continue
                        self._logEvent(
                            "debug",
                            "dispatch.skip.partition_busy",
                            wid=wid,
                            wf=str(row.get("workflow_name")),
                            pkey=pkey,
                        )
                        continue
                except Exception as e:
                    self._logEvent(
                        "error",
                        "dispatch.skip.partition_busy.error",
                        wid=wid,
                        wf=str(row.get("workflow_name")),
                        pkey=pkey,
                        msg=str(e),
                    )
                    continue

            added_wf = False
            try:
                added_wf = bool(self._activeWorkflows.add(wid))
            except:
                added_wf = True
            if pkey is not None:
                try:
                    self._activePartitions.add(pkey)
                except:
                    pass
            self._inFlightWorkflows.incrementAndGet()
            self._logEvent(
                "info",
                "wf.dispatch",
                wid=wid,
                wf=str(row.get("workflow_name")),
                pkey=pkey,
                queue=queueName,
            )
            try:
                self._workflowExecutor.execute(_RunWorkflow(self, row))
            except Exception as e:
                self._logEvent("error", "wf.dispatch.error", wid=wid, msg=str(e))
                try:
                    released = int(self.db.releaseClaim(wid))
                    if released != 1:
                        self._logEvent(
                            "error",
                            "wf.dispatch.releaseClaim_failed",
                            wid=wid,
                            released=released,
                        )
                except Exception as releaseErr:
                    self._logEvent(
                        "error",
                        "wf.dispatch.releaseClaim_error",
                        wid=wid,
                        msg=str(releaseErr),
                    )
                try:
                    if added_wf:
                        self._activeWorkflows.remove(wid)
                except:
                    pass
                if pkey is not None:
                    try:
                        self._activePartitions.remove(pkey)
                    except:
                        pass
                self._inFlightWorkflows.decrementAndGet()

        return len(claimed)

    # ----------------------------
    # cancellation
    # ----------------------------
    def cancelWorkflow(self, workflowId, reason="", actor=None):
        """
        Request cooperative cancellation for a workflow id.

        Args:
            workflow_id (str): Workflow id (UUID string).
            reason (str): Human-readable reason for logs and status payloads.
            actor (str|None): Optional actor label for future audit use.

        Returns:
            int: Number of workflow rows updated to CANCELLED.
        """
        return self.db.cancelWorkflow(workflowId, reason)

    # ----------------------------
    # retention
    # ----------------------------
    def applyRetention(self):
        """
        Apply retention policies from `workflows.retention_config` (row id=1).

        Policy fields:
        - `global_timeout_hours`: cancel active workflows older than threshold
        - `time_threshold_hours`: delete terminal runs older than threshold
        - `rows_threshold`: cap workflow_status row count by deleting oldest terminals

        Returns:
            None: Commits retention updates or rolls back on error.
        """
        tx = self.db.begin()
        try:
            cfg = self.db.getRetentionConfig(tx=tx)
            self.db.enforceGlobalTimeout(cfg.get("global_timeout_hours"), tx=tx)
            self.db.deleteOldTerminal(cfg.get("time_threshold_hours"), tx=tx)
            self.db.enforceRowsThreshold(cfg.get("rows_threshold"), tx=tx)
            self.db.commit(tx)
        except Exception as e:
            self.db.rollback(tx)
            log.warn("Retention apply failed: %s" % e)
        finally:
            self.db.close(tx)

    # ----------------------------
    # step runner
    # ----------------------------
    def _runStep(self, stepName, fn, options, args, kwargs):
        """
        Execute one step with deterministic call sequencing and durable replay.

        Behavior:
        - deterministic `callSequence` per workflow execution context
        - cache/replay from `workflows.operation_outputs`
        - DBOS-style retry/backoff policy from step options

        Args:
            stepName (str): Step registry name.
            fn (callable): Step callable.
            options (dict|None): Step execution/retry options.
            args (tuple): Positional step args.
            kwargs (dict): Keyword step args.

        Returns:
            object: Step return value (new execution or replayed cache value).
        """
        ctx = WorkflowsRuntime.getCurrentContext()
        if ctx is None:
            raise Exception("Step called without workflow context")

        self.checkCancelled()
        self.checkTimeout()

        callSequence = ctx.nextCallSeq()
        t0 = nowMs()

        cached = self.db.getStepOutput(ctx.workflowId, callSequence)
        if cached is not None:
            cachedStatus = str(cached.get("status") or "")
            if cachedStatus == settings.STATUS_SUCCESS:
                self._logEvent(
                    "debug",
                    "step.cache_hit",
                    wid=ctx.workflowId,
                    wf=ctx.workflowName,
                    step=stepName,
                    call_seq=callSequence,
                )
                dt = nowMs() - t0
                self._logEvent(
                    "info",
                    "step.success",
                    wid=ctx.workflowId,
                    wf=ctx.workflowName,
                    step=stepName,
                    call_seq=callSequence,
                    dur_ms=dt,
                    replay=True,
                )
                return cached.get("output")
            if cachedStatus == settings.STATUS_ERROR:
                errObj = cached.get("error")
                errMsg = ""
                if isinstance(errObj, dict):
                    errMsg = str(errObj.get("message") or "")
                if not errMsg:
                    errMsg = _safe(errObj)
                raise Exception(
                    "Replayed step error step=%s call_seq=%s message=%s"
                    % (stepName, callSequence, errMsg)
                )

        opts = options or {}
        retriesAllowed = bool(opts.get("retries_allowed", False))
        intervalSeconds = float(opts.get("interval_seconds", 0.0) or 0.0)
        maxAttempts = int(opts.get("max_attempts", 1) or 1)
        backoff = float(opts.get("backoff_rate", 1.0) or 1.0)

        if not retriesAllowed:
            maxAttempts = 1

        started = nowMs()
        try:
            self.db.insertStepAttempt(ctx.workflowId, callSequence, stepName, started)
        except:
            pass

        res, attemptsUsed = self._executeWithRetry(
            fn=fn,
            attempts=maxAttempts,
            intervalS=intervalSeconds,
            backoffRate=backoff,
            wid=ctx.workflowId,
            wfName=ctx.workflowName,
            stepName=stepName,
            callSeq=callSequence,
            args=args,
            kwargs=kwargs,
        )
        self.db.updateStepSuccess(
            ctx.workflowId, callSequence, res, nowMs(), int(attemptsUsed)
        )
        self._logEvent(
            "info",
            "step.success",
            wid=ctx.workflowId,
            wf=ctx.workflowName,
            step=stepName,
            call_seq=callSequence,
            attempts=attemptsUsed,
            dur_ms=(nowMs() - t0),
            replay=False,
        )
        return res

    def _executeClaimedRow(self, row):
        """
        Execute a claimed workflow row and persist terminal status safely.

        How it behaves:
        - marks RUNNING only when the row is still PENDING and claimed by this executor
        - normalizes inputs into `{resolved, raw}` with timeout control metadata
        - writes SUCCESS/ERROR/CANCELLED through terminal-safe DB updates

        Args:
            row (dict): Claimed `workflow_status` row.

        Returns:
            None: Persists workflow side effects and terminal status.
        """
        wid = str(row.get("workflow_id"))
        workflowName = str(row.get("workflow_name"))
        pkey = row.get("partition_key")
        if pkey is not None:
            pkey = str(pkey)

        self._logEvent("info", "wf.start", wid=wid, wf=workflowName, pkey=pkey)

        inputsJson = row.get("inputs_json")
        parsed_inputs = {}
        try:
            parsed_inputs = json.loads(inputsJson) if inputsJson else {}
        except:
            parsed_inputs = {}
        normalized_inputs = _normalizeInputs(parsed_inputs)

        deadline_ms = row.get("deadline_epoch_ms")
        timeout_s = _timeoutFromInputs(normalized_inputs)
        timeout_ms = None
        if timeout_s is not None:
            timeout_ms = long(float(timeout_s) * 1000.0)
        run_started_ms = nowMs()
        marked = self.db.markRunningIfClaimed(
            wid,
            self.executor_id,
            startedMs=run_started_ms,
            timeoutMs=timeout_ms,
        )
        if int(marked or 0) != 1:
            self._logEvent(
                "warn",
                "wf.start.skipped",
                wid=wid,
                wf=workflowName,
                reason="claim_state_mismatch",
            )
            return

        if deadline_ms is not None:
            deadline_ms = deadline_ms
        elif timeout_ms is not None and timeout_ms > 0:
            deadline_ms = run_started_ms + timeout_ms

        resolved = normalized_inputs.get("resolved")
        ctx = _Context(
            workflowId=wid,
            workflowName=workflowName,
            inputsObj=normalized_inputs,
            deadlineEpochMs=deadline_ms,
        )

        WorkflowsRuntime._runtimeThreadLocal.set(self)
        WorkflowsRuntime._contextThreadLocal.set(ctx)
        try:
            fn = self.getWorkflowFn(workflowName)
            if isinstance(resolved, dict):
                result = fn(**resolved)
            else:
                result = fn(resolved)
            self.checkCancelled()
            self.checkTimeout()
            updated = self._recordWorkflowTerminal(
                wid,
                settings.STATUS_SUCCESS,
                fields={
                    "result_json": self._serializeResult(result),
                    "completed_at_epoch_ms": nowMs(),
                    "heartbeat_at_epoch_ms": nowMs(),
                },
            )
            if int(updated or 0) > 0:
                self._logEvent("info", "wf.success", wid=wid, wf=workflowName)
            else:
                self._logEvent(
                    "warn",
                    "wf.success.skipped_terminal",
                    wid=wid,
                    wf=workflowName,
                )
        except Cancelled:
            self._logEvent("warn", "wf.cancelled", wid=wid, wf=workflowName)
            self._recordWorkflowTerminal(
                wid,
                settings.STATUS_CANCELLED,
                fields={
                    "completed_at_epoch_ms": nowMs(),
                    "heartbeat_at_epoch_ms": nowMs(),
                },
            )
        except Exception as e:
            self._logEvent(
                "error", "wf.exception", wid=wid, wf=workflowName, msg=str(e)
            )
            err = self._serializeError(e, errType="workflow_error")
            updated = self._recordWorkflowTerminal(
                wid,
                settings.STATUS_ERROR,
                fields={
                    "error_json": json.dumps(err, separators=(",", ":")),
                    "completed_at_epoch_ms": nowMs(),
                    "heartbeat_at_epoch_ms": nowMs(),
                },
            )
            if int(updated or 0) == 0:
                self._logEvent(
                    "warn",
                    "wf.error.skipped_terminal",
                    wid=wid,
                    wf=workflowName,
                )
        finally:
            try:
                WorkflowsRuntime._contextThreadLocal.clear()
                WorkflowsRuntime._runtimeThreadLocal.clear()
            except:
                pass

    def _cancelExpiredRuns(self):
        """
        Cancel workflows with expired deadlines inside one transaction.

        Returns:
            None: Updates CANCELLED state for expired workflows in one transaction.
        """
        tx = self.db.begin()
        try:
            self.db.cancelExpired(tx=tx)
            self.db.commit(tx)
        except:
            self.db.rollback(tx)
            raise
        finally:
            self.db.close(tx)

    def _logEvent(self, level, event, **kv):
        """
        Write structured log lines.

        Args:
            level (str): Logging level (`trace`, `debug`, `info`, `warn`, `error`).
            event (str): Stable event name.
            **kv (dict): Additional structured fields for the log line.

        Returns:
            None: Tries to return a structured log line.
        """
        kv["evt"] = event
        kv["exec"] = self.executor_id
        kv["gen"] = self._generation.get()
        kv["thr"] = _threadName()
        msg = _keyValueToString(kv)
        try:
            if level == "debug":
                log.debug(msg)
            elif level == "warn":
                log.warn(msg)
            elif level == "error":
                log.error(msg)
            elif level == "trace":
                log.trace(msg)
            else:
                log.info(msg)
        except:
            pass

    def _executorDiagnostics(self, ex):
        """
        Tries to read diagnostic metrics from one Java executor in a safe way.
        (there might be a better way to accomplish this, I will keep it this way for now)

        Args:
            ex (ThreadPoolExecutor): Executor to inspect.

        Returns:
            dict: JSON-safe executor diagnostics with best-effort fields.
        """
        data = {}
        if ex is None:
            return data
        try:
            data["className"] = ex.getClass().getName()
        except:
            data["className"] = ""
        try:
            data["poolSize"] = int(ex.getPoolSize())
        except:
            data["poolSize"] = 0
        try:
            data["corePoolSize"] = int(ex.getCorePoolSize())
        except:
            data["corePoolSize"] = 0
        try:
            data["maxPoolSize"] = int(ex.getMaximumPoolSize())
        except:
            data["maxPoolSize"] = 0
        try:
            data["largestPoolSize"] = int(ex.getLargestPoolSize())
        except:
            data["largestPoolSize"] = 0
        try:
            data["activeCount"] = int(ex.getActiveCount())
        except:
            data["activeCount"] = 0
        try:
            q = ex.getQueue()
            data["queueSize"] = int(q.size())
            try:
                data["queueClassName"] = q.getClass().getName()
            except:
                data["queueClassName"] = ""
            try:
                data["queueRemainingCapacity"] = int(q.remainingCapacity())
            except:
                data["queueRemainingCapacity"] = None
        except:
            data["queueSize"] = 0
            data["queueClassName"] = ""
            data["queueRemainingCapacity"] = None
        try:
            data["taskCount"] = long(ex.getTaskCount())
        except:
            data["taskCount"] = 0
        try:
            data["completedTaskCount"] = long(ex.getCompletedTaskCount())
        except:
            data["completedTaskCount"] = 0
        try:
            data["isShutdown"] = bool(ex.isShutdown())
        except:
            data["isShutdown"] = False
        try:
            data["isTerminated"] = bool(ex.isTerminated())
        except:
            data["isTerminated"] = False
        try:
            data["isTerminating"] = bool(ex.isTerminating())
        except:
            data["isTerminating"] = None
        return data

    def getExecutorDiagnostics(self, includeSamples=False, sampleLimit=20):
        """
        Return a detailed formatted runtime diagnostic snapshot.

        This is intended for admin troubleshooting and can be bound
        directly to Perspective views without additional serialization work.

        Args:
            includeSamples (bool): Include sampled active workflow/partition IDs when True.
            sampleLimit (int): Maximum sample size for each sampled collection.

        Returns:
            dict: diagnostic split into identity/workload/executors/inMemoryQueue/capacity.
        """
        now_ms = nowMs()
        projectName = system.util.getProjectName()

        workflow_executor = self._executorDiagnostics(self._workflowExecutor)
        step_executor = self._executorDiagnostics(self._stepExecutor)

        workers = self._workflowWorkers.get()
        active = int(workflow_executor.get("activeCount") or 0)
        queued = int(workflow_executor.get("queueSize") or 0)
        available = workers - (active + queued)
        if available < 0:
            available = 0

        payload = {
            "identity": {
                "projectName": projectName,
                "dbName": self.dbName,
                "executorId": self.executor_id,
                "generation": self._generation.get(),
                "timestampMs": now_ms,
                "threadName": _threadName(),
            },
            "maintenance": {
                "enabled": self._maintenanceEnabled.get(),
                "mode": self._maintenanceMode,
                "reason": str(self._maintenanceReason or ""),
                "lastSwapAtMs": self._lastSwapAtMs.get(),
            },
            "workload": {
                "inFlightWorkflows": self._inFlightWorkflows.get(),
                "activeWorkflowsCount": _getSetSize(self._activeWorkflows),
                "activePartitionsCount": _getSetSize(self._activePartitions),
            },
            "executors": {
                "workflows": workflow_executor,
                "steps": step_executor,
            },
            "inMemoryQueue": {
                "type": self._inMemoryQueue.getClass().getName(),
                "depth": self._inMemoryQueue.size(),
                "maxDepth": self._inMemoryQueueMaxDepth.get(),
                "overflowMode": self._getInMemoryQueueOverflowMode(),
                "overflowCount": self._inMemoryQueueOverflowCount.get(),
                "lastOverflowAtMs": self._inMemoryQueueLastOverflowAtMs.get(),
                "acceptDuringMaintenance": self._inMemoryQueueAcceptDuringMaintenance.get(),
                "flushDuringMaintenance": self._inMemoryQueueFlushDuringMaintenance.get(),
            },
            "capacity": {
                "workflowWorkersConfigured": workers,
                "workflowActiveCount": active,
                "workflowQueueSize": queued,
                "workflowAvailableSlots": available,
            },
        }

        return payload


class _RunWorkflow(Runnable):
    """Runnable wrapper that executes one claimed workflow row."""

    def __init__(self, rt, row):
        """
        Bind a runtime facade and claimed DB row to this runnable.

        Args:
            rt (WorkflowsRuntime): Runtime facade executing the workflow.
            row (dict): Claimed workflow_status row.

        Returns:
            None: Stores constructor arguments on `self`.
        """
        self.rt = rt
        self.row = row

    def run(self):
        """
        Set worker thread context, delegate execution, then release runtime locks.
        """
        wid = str(self.row.get("workflow_id"))
        pkey = self.row.get("partition_key")
        if pkey is not None:
            pkey = str(pkey)

        thr = Thread.currentThread()
        old_name = None
        try:
            old_name = thr.getName()
        except:
            old_name = "wf"
        new_name = old_name
        try:
            proj = system.util.getProjectName().lower()
            exec_short = str(self.rt.executor_id)
            if len(exec_short) > 8:
                exec_short = exec_short[-8:]
            new_name = "wf[%s.%s.g%s.%s]" % (
                proj,
                exec_short,
                self.rt._generation.get(),
                wid,
            )
            thr.setName(new_name)
        except:
            pass

        try:
            self.rt._executeClaimedRow(self.row)
        finally:
            try:
                self.rt._activeWorkflows.remove(wid)
            except:
                pass
            if pkey is not None:
                try:
                    self.rt._activePartitions.remove(pkey)
                except:
                    pass
            try:
                self.rt._inFlightWorkflows.decrementAndGet()
            except:
                pass
            try:
                thr.setName(old_name)
            except:
                pass
