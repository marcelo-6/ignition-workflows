# exchange/workflows/tests/fixtures.py
"""
Test fixtures for the workflows.

These workflows and steps are intentionally small and explicit so tests can assert
engine behavior (retry, replay, maintenance, mailbox arbitration, timeout, etc.)
without guessing at side effects.

"""

import time

from exchange.workflows.engine.runtime import workflow, step
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.runtime import WorkflowsRuntime

log = system.util.getLogger(
    "exchange." + system.util.getProjectName().lower() + ".workflows.tests.fixtures"
)

_COUNTER_PREFIX = "exchange.workflows.tests.counter"
_COUNTER_NAMES = (
    "unstable_step",
    "always_fail_step",
    "counting_step",
)


def _get_counter_store():
    """
    Return a persistent key/value store shared across test runs.

    The harness prefers `system.util.globalVarMap` when available and falls back
    to `system.util.getGlobals()` so counters survive script scope changes.

    Returns:
            tuple: `(store, store_type)` where `store` is mutable or `None`.
    """
    try:
        if hasattr(system.util, "globalVarMap"):
            return system.util.globalVarMap(_COUNTER_PREFIX), "globalVarMap"
    except Exception as e:
        log.warn("globalVarMap unavailable; falling back to getGlobals(): %s" % e)
    try:
        return system.util.getGlobals(), "globals"
    except:
        return None, "none"


def _store_get(store, key, default=None):
    """
    Read one value from a Java/Python map with safe fallbacks.

    Args:
            store (object): Persistent map-like object.
            key (str): Counter key.
            default (object|None): Fallback value when missing.

    Returns:
            object: Stored value or `default`.
    """
    if store is None:
        return default
    try:
        v = store.get(key)
        if v is None:
            return default
        return v
    except:
        pass
    try:
        return store[key]
    except:
        return default


def _store_set(store, key, value):
    """
    Write one value to a Java/Python map with safe fallbacks.

    Args:
            store (object): Persistent map-like object.
            key (str): Counter key.
            value (object): Value to store.

    Returns:
            None: Updates shared counter storage.
    """
    if store is None:
        return
    try:
        store[key] = value
        return
    except:
        pass
    try:
        store.put(key, value)
    except:
        pass


def _counter_key(name):
    """
    Build a namespaced key for one fixture counter.

    Args:
            name (str): Logical counter name.

    Returns:
            str: Fully-qualified storage key.
    """
    return "%s.%s" % (_COUNTER_PREFIX, str(name or ""))


def _inc_counter(name):
    """
    Increment and return a named fixture counter.

    Args:
            name (str): Logical counter name.

    Returns:
            int: New counter value.
    """
    store, _store_type = _get_counter_store()
    key = _counter_key(name)
    val = int(_store_get(store, key, default=0) or 0) + 1
    _store_set(store, key, val)
    return val


def get_counter(name):
    """
    Read a named fixture counter.

    Args:
            name (str): Logical counter name.

    Returns:
            int: Current counter value, defaulting to `0`.
    """
    store, _store_type = _get_counter_store()
    return int(_store_get(store, _counter_key(name), default=0) or 0)


def _reset_counter(name):
    """
    Reset one named fixture counter.

    Args:
            name (str): Logical counter name.

    Returns:
            None: Removes the key when present.
    """
    store, _store_type = _get_counter_store()
    key = _counter_key(name)
    if store is None:
        return
    try:
        del store[key]
    except:
        pass
    try:
        store.remove(key)
    except:
        pass


# ----------------------------
# Steps
# ----------------------------


@step(
    name="tests.unstable_step",
    options={
        "retries_allowed": True,
        "interval_seconds": 1,
        "max_attempts": 5,
        "backoff_rate": 1.0,
    },
)
def unstable_step(fail_until=2):
    """
    Fail until a shared counter passes `fail_until`.

    This gives the harness a deterministic way to verify retry/backoff behavior.

    Args:
            fail_until (int): Number of failed attempts before success.

    Returns:
            dict: `{ok: True, attempt: <int>}` once retries reach success.

    Raises:
            Exception: While attempt count is still below or equal to `fail_until`.
    """
    n = _inc_counter("unstable_step")
    if n <= int(fail_until):
        raise Exception("Intentional failure #%d" % n)
    return {"ok": True, "attempt": n}


@step(
    name="tests.fast_side_effect_step",
    options={"retries_allowed": False, "max_attempts": 1},
)
def fast_side_effect_step(payload=None):
    """
    Write one log stream entry and return quickly.

    Args:
            payload (dict|None): Optional data to record.

    Returns:
            dict: Echo payload and success marker.
    """
    rt = WorkflowsRuntime.getCurrentRuntime()
    if rt is not None:
        rt.checkCancelled()
        rt.writeStream("log", {"message": "fast_side_effect_step", "payload": payload})
    return {"ok": True, "payload": payload}


@step(
    name="tests.always_fail_step",
    options={"retries_allowed": False, "max_attempts": 1},
)
def always_fail_step(marker=None):
    """
    Always fail after incrementing a persistent counter.

    This is used by replay tests to prove the step body does not run again when
    a cached ERROR is replayed.

    Args:
            marker (str|None): Optional marker for easier log/debug output.

    Returns:
            None: This step always raises.

    Raises:
            Exception: Always.
    """
    n = _inc_counter("always_fail_step")
    raise Exception("Fixture forced step failure marker=%s call=%s" % (marker, n))


@step(
    name="tests.counting_step",
    options={"retries_allowed": False, "max_attempts": 1},
)
def counting_step(payload=None):
    """
    Increment a call counter and return payload.

    This is used by replay tests to prove SUCCESS replays avoid re-running step code.

    Args:
            payload (object|None): Payload to return.

    Returns:
            dict: Includes call count and echoed payload.
    """
    n = _inc_counter("counting_step")
    return {"count": n, "payload": payload}


# ----------------------------
# Workflows
# ----------------------------


@workflow(name="tests.retry_workflow")
def retry_workflow(fail_until=4):
    """
    Run one retrying step and finish with a success payload.

    Args:
            fail_until (int): Attempt number threshold before step succeeds.

    Returns:
            dict: Wrapped step result.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    res = unstable_step(fail_until=fail_until)
    rt.writeStream("log", {"message": "retry_workflow complete", "res": res})
    rt.setEvent("phase.state", "COMPLETE")
    return {"result": res}


@workflow(name="tests.hold_resume")
def hold_resume(hold_seconds=2):
    """
    Exercise HOLD/RESUME/STOP command handling through mailbox reads.

    Args:
            hold_seconds (float): How long to wait for HOLD before finishing naturally.

    Returns:
            dict: Result describing held/resumed/stopped path.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    start = system.date.toMillis(system.date.now())
    deadline = start + int(float(hold_seconds) * 1000.0)

    while system.date.toMillis(system.date.now()) < deadline:
        cmd_info = rt.recvCommand(currentState="RUNNING", topic="cmd", timeoutSeconds=0)
        if cmd_info and cmd_info.get("cmd") == "HOLD":
            rt.setEvent("phase.state", "HELD")
            rt.writeStream("log", {"message": "entered HELD"})
            while True:
                held_cmd = rt.recvCommand(
                    currentState="HELD", topic="cmd", timeoutSeconds=0.2
                )
                if held_cmd:
                    c = held_cmd.get("cmd")
                    if c == "RESUME":
                        rt.setEvent("phase.state", "RUNNING")
                        rt.writeStream("log", {"message": "resumed"})
                        rt.setEvent("phase.state", "COMPLETE")
                        return {"ok": True, "resumed": True}
                    if c == "STOP":
                        rt.setEvent("phase.state", "STOPPED")
                        rt.writeStream("log", {"message": "stopped"})
                        return {"ok": True, "stopped": True}
        time.sleep(0.1)

    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "held": False}


@workflow(name="tests.command_priority_probe")
def command_priority_probe(state="RUNNING", wait_s=2.0, max_messages=5):
    """
    Receive queued commands once and report the chosen command.

    The harness sends multiple commands before the first tick to verify priority
    arbitration (`STOP > HOLD > RESUME`) through `recvCommand()`.

    Args:
            state (str): State passed to command policy checks.
            wait_s (float): Blocking wait for first command.
            max_messages (int): Max mailbox messages considered for arbitration.

    Returns:
            dict: `{"chosen": <cmd|None>, "state": <state>}`.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    chosen = rt.recvCommand(
        currentState=state,
        topic="cmd",
        timeoutSeconds=float(wait_s),
        maxMessages=int(max_messages),
    )
    picked = chosen.get("cmd") if isinstance(chosen, dict) else None
    rt.setEvent("phase.command", picked)
    rt.setEvent("phase.state", "COMPLETE")
    return {"chosen": picked, "state": state}


@workflow(name="tests.partition_sleep")
def partition_sleep(sleep_ms=1500):
    """
    Sleep for a fixed time to test partition serialization behavior.

    Args:
            sleep_ms (int): Sleep duration in milliseconds.

    Returns:
            dict: Sleep metadata.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    time.sleep(float(sleep_ms) / 1000.0)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "slept_ms": sleep_ms}


@workflow(name="tests.cancel_cooperative")
def cancel_cooperative(sleep_s=5):
    """
    Loop with cooperative cancel checks so cancel tests can stop it quickly.

    Args:
            sleep_s (float): Approximate runtime before natural completion.

    Returns:
            dict: Completion payload when not cancelled.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    rt.writeStream("log", {"message": "cancel_cooperative started", "sleep_s": sleep_s})
    deadline = system.date.addSeconds(system.date.now(), int(sleep_s))
    while system.date.isBefore(system.date.now(), deadline):
        rt.checkCancelled()
        time.sleep(0.1)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True}


@workflow(name="tests.timeout_short")
def timeout_short(sleep_s=5):
    """
    Long loop used with runtime timeouts to verify deadline cancellation.

    Args:
            sleep_s (float): Approximate runtime before natural completion.

    Returns:
            dict: Completion payload when timeout does not trigger.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    rt.writeStream("log", {"message": "timeout_short started", "sleep_s": sleep_s})
    deadline = system.date.addSeconds(system.date.now(), int(sleep_s))
    while system.date.isBefore(system.date.now(), deadline):
        rt.checkCancelled()
        time.sleep(0.1)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True}


@workflow(name="tests.step_persist_smoke")
def step_persist_smoke():
    """
    Call one simple step so tests can inspect operation_outputs persistence.

    Returns:
            dict: Step output wrapper.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    out = fast_side_effect_step(payload={"hello": "world"})
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "out": out}


@workflow(name="tests.replay_error_workflow")
def replay_error_workflow(marker="default"):
    """
    Call a step that always fails so replay behavior can be verified.

    Args:
            marker (str): Marker included in step exception text.

    Returns:
            None: This workflow fails by design.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    always_fail_step(marker=marker)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True}


@workflow(name="tests.replay_success_workflow")
def replay_success_workflow(payload=None):
    """
    Call a counted step so SUCCESS replay can be proven.

    Args:
            payload (object|None): Payload passed through step execution.

    Returns:
            dict: Step output.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    out = counting_step(payload=payload)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "out": out}


@workflow(name="tests.nonserializable_result")
def nonserializable_result():
    """
    Return a non-JSON-serializable value to exercise result serialization fallback.

    Returns:
            dict: Contains a Python set, which runtime serializer must handle gracefully.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    rt.setEvent("phase.state", "COMPLETE")
    return {"bad": set([1, 2, 3])}


@workflow(name="tests.maintenance_long_running")
def maintenance_long_running(sleep_s=20, heartbeat_ms=200):
    """
    Stay alive in a cooperative loop for maintenance/capacity tests.

    Args:
            sleep_s (float): Runtime upper bound in seconds.
            heartbeat_ms (int): Event heartbeat cadence in milliseconds.

    Returns:
            dict: Sleep metadata.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    started = system.date.toMillis(system.date.now())
    deadline = started + int(float(sleep_s) * 1000.0)
    pulse = max(50, int(heartbeat_ms))
    while system.date.toMillis(system.date.now()) < deadline:
        rt.checkCancelled()
        rt.setEvent("phase.heartbeat_ms", system.date.toMillis(system.date.now()))
        time.sleep(float(pulse) / 1000.0)
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "sleep_s": sleep_s}


@workflow(name="tests.fast_enqueue_target")
def fast_enqueue_target(value=None):
    """
    Minimal workflow used by in-memory queue enqueue/flush tests.

    Args:
            value (object|None): Payload copied to stream and result.

    Returns:
            dict: Echo result.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", "RUNNING")
    rt.writeStream("log", {"message": "fast_enqueue_target", "value": value})
    rt.setEvent("phase.state", "COMPLETE")
    return {"ok": True, "value": value}


def reset_fixtures():
    """
    Reset mutable fixture state before a suite run.

    Returns:
            None: Clears all known test counters.
    """
    for name in _COUNTER_NAMES:
        _reset_counter(name)
