# exchange/workflows/tests/harness.py
"""
Ignition-native test harness for workflows.

This harness is intentionally integration-heavy because the runtime behavior we
care about lives at the boundary between:
- API envelopes
- Java kernel persistence
- engine dispatch/claim lifecycle
- Postgres workflow rows

No pytest is required. The harness returns a report dict and can also render
plain text or markdown for Script Console / Perspective use.
"""

import json
import time
import traceback

from java.util.concurrent import ConcurrentHashMap, ConcurrentLinkedQueue
from java.util.concurrent.atomic import AtomicBoolean, AtomicInteger, AtomicLong

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.db import DB, uuid4, now_ms
from exchange.workflows.engine.runtime import WorkflowsRuntime
from exchange.workflows.api import service as service_api
from exchange.workflows.api import admin as admin_api
from exchange.workflows.util import response as response_util
from exchange.workflows.is88 import models as isa88_models
import exchange.workflows.engine.instance as instance_mod
import exchange.workflows.tests.fixtures as fixtures

log = system.util.getLogger(settings.getLoggerName("tests.harness"))

# ----------------------------
# Runtime budgets
# ----------------------------
TEST_WORKFLOW_TIMEOUT_S = 8.0
TEST_CASE_CLEANUP_TICK_S = 2.0
TEST_SWEEP_TICK_S = 3.0

_TERMINAL = tuple(settings.WORKFLOW_TERMINAL_STATUSES)
_ACTIVE = (
    settings.STATUS_ENQUEUED,
    settings.STATUS_PENDING,
    settings.STATUS_RUNNING,
)


# ----------------------------
# Generic helpers
# ----------------------------
def _now_ms():
    """
    Return current gateway epoch milliseconds.

    Returns:
            long: Current epoch milliseconds.
    """
    return system.date.toMillis(system.date.now())


def _safe_loads(raw):
    """
    Parse JSON text safely.

    Args:
            raw (str|None): JSON text.

    Returns:
            object|None: Parsed object, or `None` when parsing fails.
    """
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except:
        return None


def _assert_envelope(resp, expect_ok=None, expect_code=None):
    """
    Validate the standard API/admin response envelope shape.

    Args:
            resp (dict): Response payload returned by service/admin modules.
            expect_ok (bool|None): Optional expected `ok` value.
            expect_code (str|None): Optional expected `code` value.

    Returns:
            None: Raises assertion errors on malformed payloads.
    """
    assert isinstance(resp, dict), "response must be a dict"
    for key in ("ok", "code", "message", "data", "meta"):
        assert key in resp, "missing envelope key: %s" % key
    assert isinstance(resp.get("ok"), bool), "ok must be bool"
    assert isinstance(resp.get("code"), basestring), "code must be string"
    assert isinstance(resp.get("message"), basestring), "message must be string"
    assert isinstance(resp.get("data"), dict), "data must be dict"
    assert isinstance(resp.get("meta"), dict), "meta must be dict"
    assert "tsMs" in resp.get("meta"), "meta.tsMs must be present"
    if expect_ok is not None:
        assert resp.get("ok") is bool(expect_ok), "expected ok=%s got=%s" % (
            expect_ok,
            resp.get("ok"),
        )
    if expect_code is not None:
        assert resp.get("code") == expect_code, "expected code=%s got=%s" % (
            expect_code,
            resp.get("code"),
        )


def _count_rows(db_name, sql, args):
    """
    Run a `COUNT(*)` query and return it as `long`.

    Args:
            db_name (str): Ignition DB connection name.
            sql (str): SQL count query.
            args (list): Prepared statement arguments.

    Returns:
            long: Count value.
    """
    ds = system.db.runPrepQuery(sql, args, db_name)
    return long(ds.getValueAt(0, 0))


def _get_status(db, workflow_id):
    """
    Read one workflow row with fields used by tests.

    Args:
            db (DB): DB adapter instance.
            workflow_id (str): Workflow UUID.

    Returns:
            dict|None: Status payload when row exists.
    """
    ds = system.db.runPrepQuery(
        "SELECT status, result_json, error_json, claimed_by, deadline_epoch_ms, "
        "started_at_epoch_ms, completed_at_epoch_ms "
        "FROM workflows.workflow_status WHERE workflow_id=?::uuid",
        [workflow_id],
        db.db_name,
    )
    if ds.getRowCount() == 0:
        return None
    return {
        "status": str(ds.getValueAt(0, 0)),
        "result_json": ds.getValueAt(0, 1),
        "error_json": ds.getValueAt(0, 2),
        "claimed_by": ds.getValueAt(0, 3),
        "deadline_epoch_ms": ds.getValueAt(0, 4),
        "started_at_epoch_ms": ds.getValueAt(0, 5),
        "completed_at_epoch_ms": ds.getValueAt(0, 6),
    }


def _wait_terminal(db, workflow_id, timeout_s=10.0):
    """
    Poll until workflow reaches a terminal status.

    Args:
            db (DB): DB adapter instance.
            workflow_id (str): Workflow UUID.
            timeout_s (float): Maximum wait time.

    Returns:
            dict|None: Terminal workflow row or `None` on timeout.
    """
    deadline = _now_ms() + int(float(timeout_s) * 1000.0)
    while _now_ms() < deadline:
        st = _get_status(db, workflow_id)
        if st and st.get("status") in _TERMINAL:
            return st
        time.sleep(0.1)
    return None


def _tick_for(db_name, seconds=2.0, queue=None, max_to_claim=10):
    """
    Drive engine ticks for a fixed duration.

    Args:
            db_name (str): Ignition DB connection name.
            seconds (float): Total tick drive time.
            queue (str|None): Queue name. Defaults to settings queue.
            max_to_claim (int): Max rows to claim per tick.

    Returns:
            None: Ticks are executed for side effects.
    """
    rt = getWorkflows(dbName=db_name)
    queue = settings.resolveQueueName(queue)
    end_ms = _now_ms() + int(float(seconds) * 1000.0)
    while _now_ms() < end_ms:
        rt.dispatch(queueName=queue, maxToClaim=int(max_to_claim))
        time.sleep(0.1)


def _tick_until_terminal(
    db_name, workflow_id, timeout_s=10.0, queue=None, max_to_claim=10
):
    """
    Tick and poll until a workflow reaches terminal state.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_id (str): Workflow UUID.
            timeout_s (float): Max wait duration.
            queue (str|None): Queue name. Defaults to settings queue.
            max_to_claim (int): Max claim count per tick.

    Returns:
            dict|None: Terminal row, or `None` on timeout.
    """
    rt = getWorkflows(dbName=db_name)
    db = DB(db_name)
    queue = settings.resolveQueueName(queue)
    deadline_ms = _now_ms() + int(float(timeout_s) * 1000.0)
    while _now_ms() < deadline_ms:
        rt.dispatch(queueName=queue, maxToClaim=int(max_to_claim))
        st = _get_status(db, workflow_id)
        if st and st.get("status") in _TERMINAL:
            return st
        time.sleep(0.1)
    return None


def _latest_workflow_id(db_name, workflow_name, created_after_ms=None):
    """
    Get latest workflow id for a workflow name.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_name (str): Workflow function name.
            created_after_ms (int|long|None): Optional lower bound.

    Returns:
            str|None: Latest workflow UUID or `None`.
    """
    sql = "SELECT workflow_id FROM workflows.workflow_status WHERE workflow_name=? "
    args = [workflow_name]
    if created_after_ms is not None:
        sql += "AND created_at_epoch_ms>=? "
        args.append(long(created_after_ms))
    sql += "ORDER BY created_at_epoch_ms DESC LIMIT 1"
    ds = system.db.runPrepQuery(sql, args, db_name)
    if ds.getRowCount() == 0:
        return None
    return str(ds.getValueAt(0, 0))


def _workflow_ids_since(db_name, workflow_name, created_after_ms):
    """
    List workflow ids for one workflow name created since timestamp.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_name (str): Workflow function name.
            created_after_ms (int|long): Lower bound for `created_at_epoch_ms`.

    Returns:
            list[str]: Matching workflow UUIDs.
    """
    ds = system.db.runPrepQuery(
        "SELECT workflow_id FROM workflows.workflow_status "
        "WHERE workflow_name=? AND created_at_epoch_ms>=? "
        "ORDER BY created_at_epoch_ms ASC",
        [workflow_name, long(created_after_ms)],
        db_name,
    )
    out = []
    for r in range(ds.getRowCount()):
        out.append(str(ds.getValueAt(r, 0)))
    return out


def _get_operation_outputs(db_name, workflow_id):
    """
    Read operation output rows for one workflow.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_id (str): Workflow UUID.

    Returns:
            list[dict]: Step output records ordered by call sequence.
    """
    ds = system.db.runPrepQuery(
        "SELECT call_seq, step_name, status, attempts, output_json, error_json "
        "FROM workflows.operation_outputs WHERE workflow_id=?::uuid ORDER BY call_seq ASC",
        [workflow_id],
        db_name,
    )
    rows = []
    for r in range(ds.getRowCount()):
        rows.append(
            {
                "call_seq": int(ds.getValueAt(r, 0)),
                "step_name": str(ds.getValueAt(r, 1)),
                "status": str(ds.getValueAt(r, 2)),
                "attempts": int(ds.getValueAt(r, 3)),
                "output_json": ds.getValueAt(r, 4),
                "error_json": ds.getValueAt(r, 5),
            }
        )
    return rows


def _assert_event_history_contains(db_name, workflow_id, key, expected_value):
    """
    Assert event history contains one expected value.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_id (str): Workflow UUID.
            key (str): Event key.
            expected_value (object): Expected value inside history payloads.

    Returns:
            None: Raises assertion error if value is missing.
    """
    ds = system.db.runPrepQuery(
        "SELECT value_json FROM workflows.workflow_events_history "
        "WHERE workflow_id=?::uuid AND key=? ORDER BY created_at_epoch_ms ASC",
        [workflow_id, key],
        db_name,
    )
    vals = []
    for r in range(ds.getRowCount()):
        v = ds.getValueAt(r, 0)
        parsed = _safe_loads(v)
        vals.append(parsed if parsed is not None else v)

    flat = []
    for v in vals:
        if isinstance(v, basestring):
            flat.append(v)
        elif isinstance(v, dict) and "value" in v:
            flat.append(v.get("value"))
    assert (
        expected_value in vals or expected_value in flat
    ), "Expected %r in key=%s history, got=%r" % (expected_value, key, flat or vals)


def _force_reenqueue(db_name, workflow_id):
    """
    Reset one workflow row to ENQUEUED for replay tests.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_id (str): Workflow UUID.

    Returns:
            int: Number of updated rows.
    """
    now = long(_now_ms())
    return system.db.runPrepUpdate(
        "UPDATE workflows.workflow_status "
        "SET status=?, claimed_by=NULL, claimed_at_epoch_ms=NULL, "
        "started_at_epoch_ms=NULL, deadline_epoch_ms=NULL, completed_at_epoch_ms=NULL, "
        "updated_at_epoch_ms=?, heartbeat_at_epoch_ms=? "
        "WHERE workflow_id=?::uuid",
        [settings.STATUS_ENQUEUED, now, now, workflow_id],
        db_name,
    )


def _count_active_workflows(db_name, workflow_name=None):
    """
    Count active workflow rows, optionally filtered by name.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_name (str|None): Optional workflow name filter.

    Returns:
            long: Active row count.
    """
    if workflow_name is None:
        sql = (
            "SELECT COUNT(*) FROM workflows.workflow_status "
            "WHERE status IN ('ENQUEUED','PENDING','RUNNING')"
        )
        args = []
    else:
        sql = (
            "SELECT COUNT(*) FROM workflows.workflow_status "
            "WHERE workflow_name=? AND status IN ('ENQUEUED','PENDING','RUNNING')"
        )
        args = [workflow_name]
    return _count_rows(db_name, sql, args)


def _get_kernel_or_none():
    """
    Read the persistent workflows kernel directly from gateway store.

    Returns:
            object|None: Kernel map when present.
    """
    try:
        store, _store_type = instance_mod._getStoreMap()
        if store is None:
            return None
        return instance_mod._getStore(store, instance_mod._KERNEL_KEY)
    except:
        return None


def _parse_suite_filter(value):
    """
    Normalize suite filter arguments to a `set`.

    Args:
            value (str|list|tuple|set|None): User-provided suite filter.

    Returns:
            set|None: Set of suite names, or `None` when no filter is provided.
    """
    if value is None:
        return None
    if isinstance(value, basestring):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return set(parts)
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            s = str(item).strip()
            if s:
                out.append(s)
        return set(out)
    s = str(value).strip()
    if not s:
        return None
    return set([s])


# ----------------------------
# Case context / cleanup
# ----------------------------
class _CaseCtx(object):
    """
    Per-case context that tracks started workflow ids.

    Using this context keeps cleanup predictable even when a test fails midway.
    """

    def __init__(self, db_name, run_started_ms):
        """
        Create a case context.

        Args:
                db_name (str): Ignition DB connection name.
                run_started_ms (int|long): Harness start timestamp.

        Returns:
                None: Stores context fields.
        """
        self.db_name = db_name
        self.run_started_ms = long(run_started_ms)
        self.started_wids = []

    def start_workflow(
        self,
        workflow_name,
        inputs_obj=None,
        queue_name=None,
        partition_key=None,
        priority=0,
        deduplication_id=None,
        application_version=None,
        timeout_seconds=None,
    ):
        """
        Start a workflow and auto-track it for case cleanup.

        Args:
                workflow_name (str): Registered workflow name.
                inputs_obj (dict|None): Workflow inputs.
                queue_name (str|None): Queue name.
                partition_key (str|None): Partition key.
                priority (int): Queue priority.
                deduplication_id (str|None): Optional dedupe id.
                application_version (str|None): Optional version stamp.
                timeout_seconds (float|None): Optional timeout override.

        Returns:
                str: Workflow UUID.
        """
        rt = getWorkflows(dbName=self.db_name)
        queue_name = settings.resolveQueueName(queue_name)
        t = (
            TEST_WORKFLOW_TIMEOUT_S
            if timeout_seconds is None
            else float(timeout_seconds)
        )
        wid = rt.enqueue(
            workflow_name,
            inputsObj=inputs_obj,
            queueName=queue_name,
            partitionKey=partition_key,
            priority=priority,
            deduplicationId=deduplication_id,
            applicationVersion=application_version,
            timeoutSeconds=t,
        )
        self.started_wids.append(wid)
        return wid


def _cancel_if_active(db_name, workflow_id, reason="test_cleanup"):
    """
    Cancel one workflow when it is still active.

    Args:
            db_name (str): Ignition DB connection name.
            workflow_id (str): Workflow UUID.
            reason (str): Cancellation reason text.

    Returns:
            bool: True when cancellation was attempted.
    """
    rt = getWorkflows(dbName=db_name)
    db = DB(db_name)
    st = _get_status(db, workflow_id)
    if st is None:
        return False
    if st.get("status") in _TERMINAL:
        return False
    count = rt.cancelWorkflow(workflow_id, reason=reason)
    if count is None or count <= 0:
        return False
    return True


def _cleanup_case(db_name, started_wids):
    """
    Best-effort cleanup for workflows started by a case.

    Args:
            db_name (str): Ignition DB connection name.
            started_wids (list[str]): Workflow ids tracked by the case.

    Returns:
            None: Active workflows are cancelled and one cleanup tick is driven.
    """
    any_cancelled = False
    for wid in started_wids:
        if _cancel_if_active(db_name, wid, reason="test_cleanup"):
            any_cancelled = True
    if any_cancelled:
        _tick_for(db_name, seconds=TEST_CASE_CLEANUP_TICK_S)


def _sweep_stragglers(db_name, run_started_ms):
    """
    Cancel any active `tests.*` workflows created during this run.

    Args:
            db_name (str): Ignition DB connection name.
            run_started_ms (int|long): Harness run start timestamp.

    Returns:
            int: Number of workflows targeted by the sweep.
    """
    rt = getWorkflows(dbName=db_name)
    ds = system.db.runPrepQuery(
        "SELECT workflow_id FROM workflows.workflow_status "
        "WHERE workflow_name LIKE 'tests.%' "
        "AND status IN ('ENQUEUED','PENDING','RUNNING') "
        "AND created_at_epoch_ms>=?",
        [long(run_started_ms)],
        db_name,
    )
    ids = []
    for r in range(ds.getRowCount()):
        ids.append(str(ds.getValueAt(r, 0)))
    if not ids:
        return 0
    for wid in ids:
        rt.cancelWorkflow(wid, reason="test_straggler_sweep")
    _tick_for(db_name, seconds=TEST_SWEEP_TICK_S)
    return len(ids)


def _register_case(cases, suite, name, fn, profiles=None, tags=None):
    """
    Register one test case with suite metadata.

    Args:
            cases (list): Mutable registration list.
            suite (str): Suite namespace (e.g. `in_memory_queue`).
            name (str): Case identifier inside suite.
            fn (callable): Case function with signature `fn(ctx)`.
            profiles (tuple|list|None): Profiles that include this case.
            tags (tuple|list|None): Optional tags.

    Returns:
            None: Appends one case dictionary.
    """
    if profiles is None:
        profiles = ("full",)
    if tags is None:
        tags = ()
    cases.append(
        {
            "suite": str(suite),
            "name": str(name),
            "id": "%s.%s" % (suite, name),
            "fn": fn,
            "profiles": tuple(profiles),
            "tags": tuple(tags),
        }
    )


def _run_case(case, db_name, run_started_ms):
    """
    Execute one case and capture timing/failure details.

    Args:
            case (dict): Registered case metadata.
            db_name (str): Ignition DB connection name.
            run_started_ms (int|long): Harness start timestamp.

    Returns:
            dict: Case execution report row.
    """
    started = _now_ms()
    ctx = _CaseCtx(db_name=db_name, run_started_ms=run_started_ms)
    try:
        case["fn"](ctx)
        return {
            "suite": case["suite"],
            "name": case["name"],
            "id": case["id"],
            "ok": True,
            "details": "",
            "duration_ms": _now_ms() - started,
            "profiles": list(case.get("profiles", ())),
            "tags": list(case.get("tags", ())),
        }
    except Exception as e:
        return {
            "suite": case["suite"],
            "name": case["name"],
            "id": case["id"],
            "ok": False,
            "details": "%s\n%s" % (str(e), traceback.format_exc()),
            "duration_ms": _now_ms() - started,
            "profiles": list(case.get("profiles", ())),
            "tags": list(case.get("tags", ())),
        }
    finally:
        try:
            _cleanup_case(db_name, ctx.started_wids)
        except Exception as e:
            try:
                log.warn("cleanup failed for %s: %s" % (case.get("id"), e))
            except:
                pass


# ----------------------------
# Main harness
# ----------------------------
def run_all(
    db_name="WorkflowsDB", profile="full", include_suites=None, exclude_suites=None
):
    """
    Run the workflows test suite and return a structured report.

    Args:
            db_name (str): Ignition DB connection name.
            profile (str): `full` or `smoke` profile selector.
            include_suites (str|list|tuple|set|None): Optional suite allow-list.
            exclude_suites (str|list|tuple|set|None): Optional suite deny-list.

    Returns:
            dict: Report with summary, suite stats, and per-case results.
    """
    db_name = settings.resolveDbName(db_name)
    profile = str(profile or "full").strip().lower()
    if profile not in ("full", "smoke"):
        profile = "full"

    include_suites = _parse_suite_filter(include_suites)
    exclude_suites = _parse_suite_filter(exclude_suites)

    fixtures.reset_fixtures()
    db = DB(db_name)
    run_started_ms = _now_ms()
    started = run_started_ms
    cases = []

    # ----------------------------
    # Registry / schema / kernel
    # ----------------------------
    def t_registry(ctx):
        """Confirm test fixtures and expected decorator registrations are present."""
        rt = getWorkflows(dbName=db_name)
        wfs = rt.listWorkflows()
        required_wfs = (
            "tests.retry_workflow",
            "tests.hold_resume",
            "tests.command_priority_probe",
            "tests.partition_sleep",
            "tests.cancel_cooperative",
            "tests.timeout_short",
            "tests.step_persist_smoke",
            "tests.replay_error_workflow",
            "tests.replay_success_workflow",
            "tests.nonserializable_result",
            "tests.maintenance_long_running",
            "tests.fast_enqueue_target",
        )
        for wf in required_wfs:
            assert wf in wfs, "missing workflow fixture: %s" % wf

        steps = rt.listSteps()
        required_steps = (
            "tests.unstable_step",
            "tests.fast_side_effect_step",
            "tests.always_fail_step",
            "tests.counting_step",
        )
        for step_name in required_steps:
            assert step_name in steps, "missing step fixture: %s" % step_name

    _register_case(
        cases,
        "core",
        "registry_fixtures_registered",
        t_registry,
        profiles=("smoke", "full"),
    )

    def t_schema_basic(ctx):
        """Check that schema metadata is readable before running behavior tests."""
        v = db.scalar(
            "SELECT value FROM workflows.schema_meta WHERE key='schema_version'", []
        )
        assert v is not None, "schema_meta.schema_version is missing"

    _register_case(
        cases,
        "core",
        "schema_basic_present",
        t_schema_basic,
        profiles=("smoke", "full"),
    )

    def t_kernel_java_only_contract(ctx):
        """Validate the persistent kernel contract uses Java types and no legacy runtime singleton."""
        rt = getWorkflows(dbName=db_name)
        assert isinstance(
            rt, WorkflowsRuntime
        ), "getWorkflows must return runtime facade"
        kernel = _get_kernel_or_none()
        assert kernel is not None, "kernel missing from persistent store"
        assert isinstance(kernel, ConcurrentHashMap), "kernel must be ConcurrentHashMap"
        assert isinstance(
            kernel.get("generation"), AtomicLong
        ), "generation must be AtomicLong"
        assert isinstance(
            kernel.get("maintenanceEnabled"), AtomicBoolean
        ), "maintenanceEnabled must be AtomicBoolean"
        assert isinstance(
            kernel.get("inFlightWorkflows"), AtomicInteger
        ), "inFlightWorkflows must be AtomicInteger"
        assert isinstance(
            kernel.get("inMemoryQueue"), ConcurrentLinkedQueue
        ), "inMemoryQueue must be ConcurrentLinkedQueue"
        assert kernel.get("wfExecutor") is not None, "wfExecutor missing"
        assert kernel.get("stepExecutor") is not None, "stepExecutor missing"

        store, _store_type = instance_mod._getStoreMap()
        legacy = instance_mod._getStore(store, instance_mod._OLD_RUNTIME_KEY)
        assert legacy is None, "legacy persisted runtime key must stay empty"

    _register_case(
        cases,
        "core",
        "kernel_java_only_contract",
        t_kernel_java_only_contract,
        profiles=("smoke", "full"),
    )

    def t_runtime_ephemeral_facade(ctx):
        """Verify each getWorkflows call returns a new facade backed by one shared kernel."""
        rt1 = getWorkflows(dbName=db_name)
        rt2 = getWorkflows(dbName=db_name)
        assert rt1 is not rt2, "expected ephemeral facades"
        assert rt1.kernel is rt2.kernel, "facades should share one kernel"
        assert (
            rt1.executor_id == rt2.executor_id
        ), "executor id should come from shared kernel"

    _register_case(
        cases,
        "core",
        "runtime_ephemeral_facade",
        t_runtime_ephemeral_facade,
        profiles=("smoke", "full"),
    )

    def t_apps_load_idempotent(ctx):
        """Confirm app module loader can be called repeatedly without side effects."""
        import exchange.workflows.apps as apps

        a = apps.__init__.load(include_tests=True)
        b = apps.__init__.load(include_tests=True)
        assert a is True and b is True, "apps.load should be idempotent"

    _register_case(
        cases,
        "core",
        "apps_load_idempotent",
        t_apps_load_idempotent,
        profiles=("smoke", "full"),
    )

    def t_pool_stats_shape(ctx):
        """Make sure pool stats payload includes expected diagnostic fields."""
        rt = getWorkflows(dbName=db_name)
        ps = rt.getExecutorDiagnostics()
        assert "identity" in ps, "pool stats missing identity section"
        assert "maintenance" in ps, "pool stats missing maintenance section"
        assert "workload" in ps, "pool stats missing workload section"
        assert "executors" in ps, "pool stats missing executors section"
        assert "inMemoryQueue" in ps, "pool stats missing inMemoryQueue section"
        assert "capacity" in ps, "pool stats missing capacity section"
        assert "workflows" in ps.get("executors", {}), "missing workflow executor stats"
        assert "steps" in ps.get("executors", {}), "missing step executor stats"

    _register_case(
        cases,
        "core",
        "pool_stats_shape",
        t_pool_stats_shape,
        profiles=("smoke", "full"),
    )

    # ----------------------------
    # API envelope + helpers
    # ----------------------------
    def t_service_envelopes_success(ctx):
        """Exercise happy-path service endpoints and enforce envelope consistency."""
        list_resp = service_api.listWorkflows(dbName=db_name)
        _assert_envelope(list_resp, expect_ok=True, expect_code="WORKFLOW_OPTIONS_OK")
        assert (
            list_resp.get("data", {}).get("count", 0) >= 1
        ), "expected at least one workflow option"

        start_resp = service_api.start(
            workflowName="tests.step_persist_smoke",
            inputs={"resolved": {}},
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        _assert_envelope(start_resp, expect_ok=True, expect_code="WORKFLOW_ENQUEUED")
        wid = start_resp.get("workflowId")
        assert wid, "start response should include workflowId"
        ctx.started_wids.append(str(wid))

        send_resp = service_api.sendCommand(
            workflowId=str(wid),
            cmd="STOP",
            reason="tests.service_envelopes_success",
            dbName=db_name,
        )
        _assert_envelope(send_resp, expect_ok=True, expect_code="COMMAND_SENT")

        cancel_resp = service_api.cancel(
            workflowId=str(wid),
            reason="tests.service_envelopes_success",
            dbName=db_name,
        )
        _assert_envelope(cancel_resp, expect_ok=True, expect_code="CANCEL_REQUESTED")

        tick_resp = service_api.dispatch(
            queueName=settings.QUEUE_DEFAULT,
            maxToClaim=5,
            dbName=db_name,
            flushMaxItems=10,
            flushMaxMs=50,
        )
        _assert_envelope(tick_resp, expect_ok=True, expect_code="DISPATCH_OK")

        anchor = _now_ms()
        enq_resp = service_api.enqueueInMemory(
            workflowName="tests.fast_enqueue_target",
            inputs={"value": "service-envelope"},
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        _assert_envelope(enq_resp)
        if enq_resp.get("ok"):
            service_api.dispatch(
                queueName=settings.QUEUE_DEFAULT,
                maxToClaim=0,
                dbName=db_name,
                flushMaxItems=50,
                flushMaxMs=100,
            )
            ids = _workflow_ids_since(db_name, "tests.fast_enqueue_target", anchor)
            for wid2 in ids:
                ctx.started_wids.append(wid2)

    _register_case(
        cases,
        "api",
        "service_envelopes_success",
        t_service_envelopes_success,
        profiles=("smoke", "full"),
    )

    def t_service_envelopes_validation_errors(ctx):
        """Verify service endpoints return stable validation codes on bad input."""
        resp = service_api.start(dbName=db_name)
        _assert_envelope(resp, expect_ok=False, expect_code="INVALID_WORKFLOW_NAME")

        resp = service_api.enqueueInMemory(dbName=db_name)
        _assert_envelope(resp, expect_ok=False, expect_code="INVALID_WORKFLOW_NAME")

        resp = service_api.sendCommand(dbName=db_name)
        _assert_envelope(resp, expect_ok=False, expect_code="INVALID_ARGUMENTS")

        resp = service_api.cancel(dbName=db_name)
        _assert_envelope(resp, expect_ok=False, expect_code="INVALID_ARGUMENTS")

    _register_case(
        cases,
        "api",
        "service_envelopes_validation_errors",
        t_service_envelopes_validation_errors,
    )

    def t_admin_envelopes_success(ctx):
        """Exercise admin endpoints in normal mode and check envelope structure."""
        resp = admin_api.checkSchema(dbName=db_name)
        _assert_envelope(resp, expect_ok=True, expect_code="SCHEMA_OK")

        resp = admin_api.getMaintenanceStatus(dbName=db_name)
        _assert_envelope(resp, expect_ok=True, expect_code="MAINTENANCE_STATUS")

        resp = admin_api.applyRetention(dbName=db_name)
        _assert_envelope(resp, expect_ok=True, expect_code="RETENTION_APPLIED")

        enter = admin_api.enterMaintenance(
            mode="drain",
            reason="tests.admin_envelopes_success",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        _assert_envelope(enter, expect_ok=True, expect_code="MAINTENANCE_ENABLED")

        exit_resp = admin_api.exitMaintenance(dbName=db_name)
        _assert_envelope(exit_resp, expect_ok=True, expect_code="MAINTENANCE_DISABLED")

    _register_case(
        cases,
        "api",
        "admin_envelopes_success",
        t_admin_envelopes_success,
        profiles=("smoke", "full"),
    )

    def t_admin_envelopes_validation_errors(ctx):
        """Verify admin validation/error codes remain stable for UI callers."""
        resp = admin_api.enterMaintenance(
            mode="not-a-mode",
            reason="tests.invalid_mode",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        _assert_envelope(resp, expect_ok=False, expect_code="INVALID_MODE")

        try:
            admin_api.exitMaintenance(dbName=db_name)
        except:
            pass
        resp = admin_api.swapIfDrained(dbName=db_name)
        _assert_envelope(resp, expect_ok=False)
        assert resp.get("code") in (
            "MAINTENANCE_REQUIRED",
            "SWAP_FAILED",
        ), "unexpected swap error code: %s" % resp.get("code")

    _register_case(
        cases,
        "api",
        "admin_envelopes_validation_errors",
        t_admin_envelopes_validation_errors,
    )

    def t_response_helpers_contract(ctx):
        """Validate response helper behavior including compatibility top-level fields."""
        started = _now_ms() - 5
        ok = response_util.makeOk(
            code="OK_TEST",
            message="ok",
            data={"value": 1},
            meta={"dbName": db_name},
            startedMs=started,
        )
        _assert_envelope(ok, expect_ok=True, expect_code="OK_TEST")
        assert ok.get("value") == 1, "compat field mirror should expose data keys"
        assert "durationMs" in ok.get(
            "meta", {}
        ), "durationMs should be present when startedMs is supplied"

        err = response_util.makeErr(
            code="ERR_TEST",
            message="bad",
            data={"why": "x"},
            meta={"dbName": db_name},
            startedMs=started,
        )
        _assert_envelope(err, expect_ok=False, expect_code="ERR_TEST")
        assert err.get("why") == "x", "compat field mirror should expose error details"

    _register_case(
        cases,
        "api",
        "response_helper_contract",
        t_response_helpers_contract,
        profiles=("smoke", "full"),
    )

    # ----------------------------
    # Command policy
    # ----------------------------
    def t_isa88_policy_matrix(ctx):
        """Check state/command policy and command priority helpers directly."""
        assert isa88_models.allowedInState("STOP", "RUNNING") is True
        assert isa88_models.allowedInState("HOLD", "RUNNING") is True
        assert isa88_models.allowedInState("RESUME", "HELD") is True
        assert isa88_models.allowedInState("RESET", "ERROR") is True
        assert isa88_models.allowedInState("RESUME", "RUNNING") is False
        assert isa88_models.commandPriority("STOP") > isa88_models.commandPriority(
            "HOLD"
        )
        assert isa88_models.commandPriority("HOLD") > isa88_models.commandPriority(
            "RESUME"
        )

        msgs = [{"cmd": "HOLD"}, {"cmd": "RESUME"}, {"cmd": "STOP"}]
        chosen = isa88_models.pickHighestPriority(msgs, state="RUNNING")
        assert isinstance(chosen, dict), "expected chosen command dict"
        assert chosen.get("cmd") == "STOP", "STOP should win priority in RUNNING state"

    _register_case(
        cases,
        "commands",
        "isa88_policy_matrix",
        t_isa88_policy_matrix,
        profiles=("smoke", "full"),
    )

    def t_recv_command_priority(ctx):
        """Ensure mailbox arbitration returns highest-priority allowed command."""
        rt = getWorkflows(dbName=db_name)
        wid = ctx.start_workflow(
            "tests.command_priority_probe",
            inputs_obj={
                "resolved": {"state": "RUNNING", "wait_s": 2.0, "max_messages": 5}
            },
            queue_name=settings.QUEUE_DEFAULT,
        )
        # Queue both commands before first tick so recv_command sees both.
        rt.send(wid, {"cmd": "HOLD", "reason": "tests.recv_priority"}, topic="cmd")
        rt.send(wid, {"cmd": "STOP", "reason": "tests.recv_priority"}, topic="cmd")
        st = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert (
            st is not None and st.get("status") == settings.STATUS_SUCCESS
        ), "priority probe workflow should complete"
        payload = _safe_loads(st.get("result_json")) or {}
        assert (
            payload.get("chosen") == "STOP"
        ), "expected STOP from priority arbitration"

    _register_case(cases, "commands", "recv_command_priority", t_recv_command_priority)

    # ----------------------------
    # Maintenance / swap
    # ----------------------------
    def t_maintenance_drain_blocks_dispatch_and_start(ctx):
        """In drain mode, default start should reject and dispatch should pause."""
        admin_api.enterMaintenance(
            mode="drain",
            reason="tests.drain_mode",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        try:
            rejected = False
            resp = service_api.start(
                workflowName="tests.step_persist_smoke",
                inputs={"resolved": {}},
                queueName=settings.QUEUE_DEFAULT,
                dbName=db_name,
            )
            _assert_envelope(resp)
            rejected = resp.get("ok") is False
            assert rejected, "start should reject in maintenance drain mode"

            rt = getWorkflows(dbName=db_name)
            wid = rt.enqueue(
                workflowName="tests.step_persist_smoke",
                inputsObj={"resolved": {}},
                queueName=settings.QUEUE_DEFAULT,
                allowDuringMaintenance=True,
                timeoutSeconds=TEST_WORKFLOW_TIMEOUT_S,
            )
            ctx.started_wids.append(wid)
            _tick_for(db_name, seconds=1.0)
            st = _get_status(db, wid)
            assert st is not None, "expected workflow row"
            assert (
                st.get("status") == settings.STATUS_ENQUEUED
            ), "dispatch should pause during maintenance drain, got %s" % st.get(
                "status"
            )
        finally:
            admin_api.exitMaintenance(dbName=db_name)

    _register_case(
        cases,
        "maintenance",
        "drain_blocks_dispatch_and_start",
        t_maintenance_drain_blocks_dispatch_and_start,
        profiles=("smoke", "full"),
    )

    def t_maintenance_cancel_cancels_queued_and_running(ctx):
        """In cancel mode, queued and running workflows should both become CANCELLED."""
        pkey = "tests.maintenance.cancel.partition"
        wid_running = ctx.start_workflow(
            "tests.maintenance_long_running",
            inputs_obj={"resolved": {"sleep_s": 20}},
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=pkey,
            timeout_seconds=30,
        )
        wid_queued = ctx.start_workflow(
            "tests.maintenance_long_running",
            inputs_obj={"resolved": {"sleep_s": 20}},
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=pkey,
            timeout_seconds=30,
        )

        _tick_for(db_name, seconds=1.0)
        st_run = _get_status(db, wid_running)
        st_q = _get_status(db, wid_queued)
        assert st_run is not None and st_run.get("status") in (
            settings.STATUS_RUNNING,
            settings.STATUS_PENDING,
        )
        assert st_q is not None and st_q.get("status") in (
            settings.STATUS_ENQUEUED,
            settings.STATUS_PENDING,
        )

        admin_api.enterMaintenance(
            mode="cancel",
            reason="tests.cancel_mode",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        try:
            _tick_for(db_name, seconds=1.5)
            st_running = _wait_terminal(db, wid_running, timeout_s=10.0)
            st_queued = _wait_terminal(db, wid_queued, timeout_s=10.0)
            assert (
                st_running is not None
                and st_running.get("status") == settings.STATUS_CANCELLED
            )
            assert (
                st_queued is not None
                and st_queued.get("status") == settings.STATUS_CANCELLED
            )
        finally:
            admin_api.exitMaintenance(dbName=db_name)

    _register_case(
        cases,
        "maintenance",
        "cancel_cancels_queued_and_running",
        t_maintenance_cancel_cancels_queued_and_running,
        profiles=("smoke", "full"),
    )

    def t_swap_if_drained_contract(ctx):
        """Swap should refuse while active, then succeed once runtime is drained."""
        before = admin_api.getMaintenanceStatus(dbName=db_name)
        _assert_envelope(before, expect_ok=True, expect_code="MAINTENANCE_STATUS")
        gen_before = long(before.get("data", {}).get("generation") or 0)

        wid = ctx.start_workflow(
            "tests.maintenance_long_running",
            inputs_obj={"resolved": {"sleep_s": 20}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=30,
        )
        _tick_for(db_name, seconds=1.0)

        admin_api.enterMaintenance(
            mode="drain",
            reason="tests.swap_contract",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        blocked = admin_api.swapIfDrained(dbName=db_name)
        _assert_envelope(blocked, expect_ok=False)
        assert blocked.get("code") in ("NOT_DRAINED", "SWAP_FAILED")

        rt = getWorkflows(dbName=db_name)
        rt.cancelWorkflow(wid, reason="tests.swap_contract")
        _tick_until_terminal(db_name, wid, timeout_s=10.0)
        _tick_for(db_name, seconds=0.6)

        ok = admin_api.swapIfDrained(dbName=db_name)
        _assert_envelope(ok)
        if ok.get("ok"):
            assert long(ok.get("data", {}).get("generation") or 0) > gen_before
        else:
            assert ok.get("code") in (
                "NOT_DRAINED",
                "SWAP_FAILED",
                "MAINTENANCE_REQUIRED",
            )

        admin_api.exitMaintenance(dbName=db_name)

    _register_case(
        cases,
        "maintenance",
        "swap_if_drained_contract",
        t_swap_if_drained_contract,
        profiles=("smoke", "full"),
    )

    # ----------------------------
    # In-memory queue
    # ----------------------------
    def t_fast_enqueue_roundtrip(ctx):
        """Verify enqueueInMemory avoids immediate DB I/O and flushes on tick."""
        before_count = _count_rows(
            db_name,
            "SELECT COUNT(*) FROM workflows.workflow_status WHERE workflow_name=?",
            ["tests.fast_enqueue_target"],
        )
        created_anchor = _now_ms()
        ack = service_api.enqueueInMemory(
            workflowName="tests.fast_enqueue_target",
            inputs={"value": "hello-fast"},
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        _assert_envelope(ack)
        assert ack.get("ok") is True, "enqueueInMemory should accept in normal mode"

        middle_count = _count_rows(
            db_name,
            "SELECT COUNT(*) FROM workflows.workflow_status WHERE workflow_name=?",
            ["tests.fast_enqueue_target"],
        )
        assert (
            middle_count == before_count
        ), "enqueueInMemory should not insert DB row immediately"

        service_api.dispatch(
            queueName=settings.QUEUE_DEFAULT,
            maxToClaim=10,
            dbName=db_name,
            flushMaxItems=100,
            flushMaxMs=200,
        )
        wid = _latest_workflow_id(
            db_name, "tests.fast_enqueue_target", created_after_ms=created_anchor
        )
        assert wid is not None, "expected flushed workflow row"
        ctx.started_wids.append(wid)
        st = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS

    _register_case(
        cases,
        "in_memory_queue",
        "enqueue_and_timer_flush_roundtrip",
        t_fast_enqueue_roundtrip,
        profiles=("smoke", "full"),
    )

    def t_fast_enqueue_rejected_in_maintenance_by_default(ctx):
        """By default, enqueueInMemory should reject while maintenance mode is active."""
        admin_api.enterMaintenance(
            mode="drain",
            reason="tests.fast_maintenance",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        try:
            ack = service_api.enqueueInMemory(
                workflowName="tests.fast_enqueue_target",
                inputs={"value": "blocked"},
                queueName=settings.QUEUE_DEFAULT,
                dbName=db_name,
            )
            _assert_envelope(ack, expect_ok=False)
            assert ack.get("code") == "IN_MEMORY_ENQUEUE_REJECTED"
            assert ack.get("data", {}).get("reason") == "maintenance"
        finally:
            admin_api.exitMaintenance(dbName=db_name)

    _register_case(
        cases,
        "in_memory_queue",
        "maintenance_rejects_by_default",
        t_fast_enqueue_rejected_in_maintenance_by_default,
        profiles=("smoke", "full"),
    )

    def t_in_memory_queue_overflow(ctx):
        """Force in-memory queue max depth to verify overflow rejection and counters."""
        rt = getWorkflows(dbName=db_name)
        old_max = int(rt._inMemoryQueueMaxDepth.get())
        old_overflow = long(rt._inMemoryQueueOverflowCount.get())
        anchor = _now_ms()

        # Drain queue before the test starts.
        while True:
            item = rt._inMemoryQueue.poll()
            if item is None:
                break

        rt._inMemoryQueueMaxDepth.set(1)
        try:
            admin_api.enterMaintenance(
                mode="drain",
                reason="tests.in_memory_queue_overflow",
                queueName=settings.QUEUE_DEFAULT,
                dbName=db_name,
            )
            ack1 = service_api.enqueueInMemory(
                workflowName="tests.fast_enqueue_target",
                inputs={"value": "bp-1"},
                queueName=settings.QUEUE_DEFAULT,
                allowDuringMaintenance=True,
                dbName=db_name,
            )
            _assert_envelope(
                ack1, expect_ok=True, expect_code="IN_MEMORY_ENQUEUE_ACCEPTED"
            )

            ack2 = service_api.enqueueInMemory(
                workflowName="tests.fast_enqueue_target",
                inputs={"value": "bp-2"},
                queueName=settings.QUEUE_DEFAULT,
                allowDuringMaintenance=True,
                dbName=db_name,
            )
            _assert_envelope(
                ack2, expect_ok=False, expect_code="IN_MEMORY_ENQUEUE_REJECTED"
            )
            assert ack2.get("data", {}).get("reason") == "overflow"

            status = admin_api.getMaintenanceStatus(dbName=db_name)
            _assert_envelope(status, expect_ok=True)
            overflow_count = long(
                status.get("data", {}).get("inMemoryQueueOverflowCount") or 0
            )
            assert overflow_count >= (
                old_overflow + 1
            ), "overflow counter should increment"
        finally:
            rt._inMemoryQueueMaxDepth.set(old_max)
            try:
                rt.flushInMemoryQueue(
                    maxItems=200,
                    maxMs=200,
                    allowDuringMaintenance=True,
                )
            except:
                pass
            try:
                admin_api.exitMaintenance(dbName=db_name)
            except:
                pass
            for wid in _workflow_ids_since(
                db_name, "tests.fast_enqueue_target", anchor
            ):
                ctx.started_wids.append(wid)

    _register_case(
        cases,
        "in_memory_queue",
        "overflow_rejects_and_counts",
        t_in_memory_queue_overflow,
    )

    def t_in_memory_queue_flush_max_items(ctx):
        """Check flush batching by asserting one tick inserts at most flushMaxItems rows."""
        anchor = _now_ms()
        rt = getWorkflows(dbName=db_name)
        admin_api.enterMaintenance(
            mode="drain",
            reason="tests.in_memory_queue_flush_max_items",
            queueName=settings.QUEUE_DEFAULT,
            dbName=db_name,
        )
        try:
            for i in range(3):
                resp = service_api.enqueueInMemory(
                    workflowName="tests.fast_enqueue_target",
                    inputs={"value": "batch-%d" % i},
                    queueName=settings.QUEUE_DEFAULT,
                    allowDuringMaintenance=True,
                    dbName=db_name,
                )
                _assert_envelope(resp, expect_ok=True)

            inserted = int(
                rt.flushInMemoryQueue(
                    maxItems=2,
                    maxMs=200,
                    allowDuringMaintenance=True,
                )
            )
            assert inserted == 2, "first flush should insert exactly 2 rows"

            inserted2 = int(
                rt.flushInMemoryQueue(
                    maxItems=20,
                    maxMs=200,
                    allowDuringMaintenance=True,
                )
            )
            assert inserted2 >= 1, "second flush should drain remaining items"
        finally:
            admin_api.exitMaintenance(dbName=db_name)

        ids = _workflow_ids_since(db_name, "tests.fast_enqueue_target", anchor)
        for wid in ids:
            ctx.started_wids.append(wid)
        assert len(ids) >= 3, "expected all three queued items to be inserted"

    _register_case(
        cases,
        "in_memory_queue",
        "flush_respects_max_items",
        t_in_memory_queue_flush_max_items,
    )

    # ----------------------------
    # Dispatch / claim semantics
    # ----------------------------
    def t_dispatch_capacity_limited_claiming(ctx):
        """Ensure one tick claims no more work than current executor capacity."""
        rt = getWorkflows(dbName=db_name)
        workers = int(rt._workflowWorkers.get())
        if workers < 1:
            workers = 1

        target = workers + 3
        for i in range(target):
            ctx.start_workflow(
                "tests.maintenance_long_running",
                inputs_obj={"resolved": {"sleep_s": 6, "heartbeat_ms": 200}},
                queue_name=settings.QUEUE_DEFAULT,
                partition_key="tests.capacity.%d" % i,
                timeout_seconds=15,
            )

        resp = service_api.dispatch(
            queueName=settings.QUEUE_DEFAULT,
            maxToClaim=target,
            dbName=db_name,
            flushMaxItems=0,
            flushMaxMs=1,
        )
        _assert_envelope(resp, expect_ok=True, expect_code="DISPATCH_OK")
        claimed = int(resp.get("data", {}).get("claimedCount") or 0)
        assert claimed <= workers, "claimed=%s workers=%s" % (claimed, workers)
        assert claimed >= 1, "expected at least one claim when queue has work"

    _register_case(
        cases,
        "dispatch",
        "capacity_limited_claiming",
        t_dispatch_capacity_limited_claiming,
    )

    def t_partition_busy_releases_claim(ctx):
        """Simulate local partition busy and verify claim is released back to ENQUEUED."""
        rt = getWorkflows(dbName=db_name)
        pkey = "tests.partition.busy.release"
        try:
            rt._activePartitions.add(pkey)
            wid = ctx.start_workflow(
                "tests.step_persist_smoke",
                inputs_obj={"resolved": {}},
                queue_name=settings.QUEUE_DEFAULT,
                partition_key=pkey,
                timeout_seconds=10,
            )
            rt.dispatch(queueName=settings.QUEUE_DEFAULT, maxToClaim=1)
            st = _get_status(db, wid)
            assert st is not None, "workflow row missing"
            assert (
                st.get("status") == settings.STATUS_ENQUEUED
            ), "partition-busy row should be re-enqueued"
            assert st.get("claimed_by") in (None, ""), "claim should be cleared"
        finally:
            try:
                rt._activePartitions.remove(pkey)
            except:
                pass

    _register_case(
        cases,
        "dispatch",
        "partition_busy_releases_claim",
        t_partition_busy_releases_claim,
    )

    def t_dispatch_failure_releases_claim(ctx):
        """Force executor rejection and verify the claimed row is cleanly released."""
        rt = getWorkflows(dbName=db_name)

        wid = ctx.start_workflow(
            "tests.step_persist_smoke",
            inputs_obj={"resolved": {}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=10,
        )

        class _FailingQueue(object):
            """Tiny queue stub so engine_tick capacity checks still run."""

            def size(self):
                """Return empty queue size for this fake executor."""
                return 0

        class _FailingExecutor(object):
            """Executor stub that always rejects dispatch."""

            def getActiveCount(self):
                """Pretend no active workers so claiming is allowed."""
                return 0

            def getQueue(self):
                """Provide a queue object with `size()` used by runtime metrics."""
                return _FailingQueue()

            def execute(self, runnable):
                """Always fail dispatch so runtime exercises release-claim path."""
                raise Exception("forced dispatch failure for test")

        old_exec = rt._workflowExecutor
        rt._workflowExecutor = _FailingExecutor()
        try:
            rt.dispatch(queueName=settings.QUEUE_DEFAULT, maxToClaim=1)
            st = _get_status(db, wid)
            assert st is not None, "workflow row missing"
            assert (
                st.get("status") == settings.STATUS_ENQUEUED
            ), "dispatch failure should release to ENQUEUED"
            assert st.get("claimed_by") in (None, ""), "claim owner should be cleared"
        finally:
            rt._workflowExecutor = old_exec

    _register_case(
        cases,
        "dispatch",
        "dispatch_failure_releases_claim",
        t_dispatch_failure_releases_claim,
    )

    def t_start_sets_started_and_deadline_at_run(ctx):
        """Confirm started/deadline timestamps are set when execution begins, not at enqueue."""
        wid = ctx.start_workflow(
            "tests.cancel_cooperative",
            inputs_obj={"resolved": {"sleep_s": 3}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=5.0,
        )
        st0 = _get_status(db, wid)
        assert st0 is not None
        assert (
            st0.get("started_at_epoch_ms") is None
        ), "started_at should be empty before run"
        assert (
            st0.get("deadline_epoch_ms") is None
        ), "deadline should be empty before run"

        _tick_for(db_name, seconds=1.0)
        st1 = _get_status(db, wid)
        assert st1 is not None
        assert (
            st1.get("started_at_epoch_ms") is not None
        ), "started_at should be set after run starts"
        assert (
            st1.get("deadline_epoch_ms") is not None
        ), "deadline should be set when running"
        delta = long(st1.get("deadline_epoch_ms") - st1.get("started_at_epoch_ms"))
        assert 3000 <= delta <= 7000, (
            "expected ~5s timeout window from start, got delta_ms=%s" % delta
        )

    _register_case(
        cases,
        "dispatch",
        "start_sets_started_and_deadline_at_run",
        t_start_sets_started_and_deadline_at_run,
    )

    # ----------------------------
    # Runtime + steps
    # ----------------------------
    def t_start_enqueues_row(ctx):
        """Start should create a workflow row and eventually complete through tick dispatch."""
        wid = ctx.start_workflow(
            "tests.step_persist_smoke",
            inputs_obj={"resolved": {}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        st = _get_status(db, wid)
        assert st is not None, "workflow_status row missing"
        assert st.get("status") in (
            settings.STATUS_ENQUEUED,
            settings.STATUS_PENDING,
            settings.STATUS_RUNNING,
            settings.STATUS_SUCCESS,
        )
        st2 = _tick_until_terminal(db_name, wid, timeout_s=8.0)
        assert st2 is not None and st2.get("status") == settings.STATUS_SUCCESS
        assert (
            st2.get("deadline_epoch_ms") is not None
        ), "deadline should be set once execution starts"

    _register_case(
        cases,
        "runtime",
        "start_enqueues_row",
        t_start_enqueues_row,
        profiles=("smoke", "full"),
    )

    def t_operation_outputs_persisted(ctx):
        """One successful step call should persist operation_outputs with SUCCESS status."""
        wid = ctx.start_workflow(
            "tests.step_persist_smoke",
            inputs_obj={"resolved": {}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        st = _tick_until_terminal(db_name, wid, timeout_s=8.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS

        outs = _get_operation_outputs(db_name, wid)
        assert len(outs) >= 1, "expected at least one operation output row"
        assert outs[0].get("status") == settings.STATUS_SUCCESS
        assert int(outs[0].get("attempts")) == 1

    _register_case(
        cases,
        "steps",
        "operation_outputs_persisted",
        t_operation_outputs_persisted,
        profiles=("smoke", "full"),
    )

    def t_retry_policy_dbos(ctx):
        """Retry fixture should eventually succeed with attempts recorded >= failure threshold."""
        fixtures.reset_fixtures()
        wid = ctx.start_workflow(
            "tests.retry_workflow",
            inputs_obj={"resolved": {"fail_until": 4}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        st = _tick_until_terminal(db_name, wid, timeout_s=12.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS

        outs = _get_operation_outputs(db_name, wid)
        assert len(outs) >= 1, "expected operation output row"
        assert outs[0].get("status") == settings.STATUS_SUCCESS
        assert int(outs[0].get("attempts")) >= 4, "expected attempts >= 4"

    _register_case(
        cases,
        "steps",
        "retry_policy_dbos",
        t_retry_policy_dbos,
        profiles=("smoke", "full"),
    )

    def t_step_error_replay_no_rerun(ctx):
        """Replaying a cached step ERROR should fail deterministically without rerunning step code."""
        fixtures.reset_fixtures()
        wid = ctx.start_workflow(
            "tests.replay_error_workflow",
            inputs_obj={"resolved": {"marker": "first"}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=20,
        )
        st1 = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st1 is not None and st1.get("status") == settings.STATUS_ERROR
        calls_before = fixtures.get_counter("always_fail_step")
        assert (
            int(calls_before) == 1
        ), "first execution should call failing step exactly once"

        rows = _force_reenqueue(db_name, wid)
        assert int(rows) == 1, "expected one row re-enqueued"
        st2 = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st2 is not None and st2.get("status") == settings.STATUS_ERROR
        calls_after = fixtures.get_counter("always_fail_step")
        assert int(calls_after) == int(
            calls_before
        ), "cached ERROR replay must not rerun step body"
        err = _safe_loads(st2.get("error_json")) or {}
        msg = str(err.get("message") or "")
        assert (
            "Replayed step error" in msg
        ), "expected deterministic replay error message"

    _register_case(
        cases, "steps", "error_replay_does_not_rerun", t_step_error_replay_no_rerun
    )

    def t_step_success_replay_no_rerun(ctx):
        """Replaying a cached SUCCESS should avoid rerunning the underlying step function."""
        fixtures.reset_fixtures()
        wid = ctx.start_workflow(
            "tests.replay_success_workflow",
            inputs_obj={"resolved": {"payload": "first"}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=20,
        )
        st1 = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st1 is not None and st1.get("status") == settings.STATUS_SUCCESS
        calls_before = fixtures.get_counter("counting_step")
        assert int(calls_before) == 1, "first run should execute counting_step once"

        rows = _force_reenqueue(db_name, wid)
        assert int(rows) == 1
        st2 = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st2 is not None and st2.get("status") == settings.STATUS_SUCCESS
        calls_after = fixtures.get_counter("counting_step")
        assert int(calls_after) == int(
            calls_before
        ), "cached SUCCESS replay must skip step execution"

    _register_case(
        cases, "steps", "success_replay_does_not_rerun", t_step_success_replay_no_rerun
    )

    def t_nonserializable_result_fallback(ctx):
        """Workflow success with non-serializable result should store serializer fallback payload."""
        wid = ctx.start_workflow(
            "tests.nonserializable_result",
            inputs_obj={"resolved": {}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        st = _tick_until_terminal(db_name, wid, timeout_s=8.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS
        result_obj = _safe_loads(st.get("result_json")) or {}
        assert (
            result_obj.get("type") == "result_serialize_error"
        ), "expected serializer fallback payload"

    _register_case(
        cases,
        "steps",
        "nonserializable_result_fallback",
        t_nonserializable_result_fallback,
    )

    # ----------------------------
    # Concurrency + mailbox + events
    # ----------------------------
    def t_mailbox_hold_resume(ctx):
        """Send HOLD then RESUME and verify workflow transitions through HELD state."""
        rt = getWorkflows(dbName=db_name)
        wid = ctx.start_workflow(
            "tests.hold_resume",
            inputs_obj={"resolved": {"hold_seconds": 3}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        _tick_for(db_name, seconds=0.8)
        rt.send(wid, {"cmd": "HOLD"}, topic="cmd")
        time.sleep(0.25)
        rt.send(wid, {"cmd": "RESUME"}, topic="cmd")
        st = _tick_until_terminal(db_name, wid, timeout_s=12.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS
        _assert_event_history_contains(db_name, wid, "phase.state", "HELD")

    _register_case(
        cases,
        "concurrency",
        "mailbox_hold_resume",
        t_mailbox_hold_resume,
        profiles=("smoke", "full"),
    )

    def t_partition_serialization(ctx):
        """Two same-partition workflows should not run at the same time."""
        wid1 = ctx.start_workflow(
            "tests.partition_sleep",
            inputs_obj={"resolved": {"sleep_ms": 1500}},
            queue_name=settings.QUEUE_DEFAULT,
            partition_key="unit:TEST",
        )
        wid2 = ctx.start_workflow(
            "tests.partition_sleep",
            inputs_obj={"resolved": {"sleep_ms": 10}},
            queue_name=settings.QUEUE_DEFAULT,
            partition_key="unit:TEST",
        )
        _tick_for(db_name, seconds=0.5)
        st1 = _get_status(db, wid1)
        st2 = _get_status(db, wid2)
        assert st1 is not None and st2 is not None
        assert st2.get("status") in (
            settings.STATUS_ENQUEUED,
            settings.STATUS_PENDING,
        ), "second same-partition workflow should not be RUNNING yet"
        st2_term = _tick_until_terminal(db_name, wid2, timeout_s=12.0)
        assert (
            st2_term is not None and st2_term.get("status") == settings.STATUS_SUCCESS
        )

    _register_case(
        cases,
        "concurrency",
        "partition_serializes_same_key",
        t_partition_serialization,
        profiles=("smoke", "full"),
    )

    def t_stream_log_written(ctx):
        """Step fixture should emit at least one log stream row."""
        wid = ctx.start_workflow(
            "tests.step_persist_smoke",
            inputs_obj={"resolved": {}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        st = _tick_until_terminal(db_name, wid, timeout_s=8.0)
        assert st is not None and st.get("status") == settings.STATUS_SUCCESS
        n = _count_rows(
            db_name,
            "SELECT COUNT(*) FROM workflows.streams WHERE workflow_id=?::uuid AND key='log'",
            [wid],
        )
        assert int(n) >= 1, "expected log stream rows"

    _register_case(
        cases,
        "concurrency",
        "streams_log_written",
        t_stream_log_written,
        profiles=("smoke", "full"),
    )

    def t_cancel_workflow(ctx):
        """Cancel path should drive workflow to CANCELLED with error payload."""
        rt = getWorkflows(dbName=db_name)
        wid = ctx.start_workflow(
            "tests.cancel_cooperative",
            inputs_obj={"resolved": {"sleep_s": 5}},
            queue_name=settings.QUEUE_DEFAULT,
        )
        _tick_for(db_name, seconds=0.7)
        rt.cancelWorkflow(wid, reason="test_cancel")
        st = _tick_until_terminal(db_name, wid, timeout_s=10.0)
        assert st is not None and st.get("status") == settings.STATUS_CANCELLED
        assert st.get("error_json") is not None, "cancel should populate error_json"

    _register_case(
        cases,
        "concurrency",
        "cancel_cooperative",
        t_cancel_workflow,
        profiles=("smoke", "full"),
    )

    def t_timeout_deadline_cancel(ctx):
        """Timeout path should cancel workflow after deadline and persist timeout error metadata."""
        wid = ctx.start_workflow(
            "tests.timeout_short",
            inputs_obj={"resolved": {"sleep_s": 5}},
            queue_name=settings.QUEUE_DEFAULT,
            timeout_seconds=1.0,
        )
        _tick_for(db_name, seconds=0.5)
        time.sleep(1.2)
        _tick_for(db_name, seconds=1.5)
        st = _wait_terminal(db, wid, timeout_s=10.0)
        assert st is not None and st.get("status") == settings.STATUS_CANCELLED
        assert st.get("error_json") is not None
        err = _safe_loads(st.get("error_json")) or {}
        assert err.get("type") in (
            "timeout",
            "cancelled",
        ), "unexpected timeout cancel type"

    _register_case(
        cases,
        "concurrency",
        "timeout_deadline_cancel",
        t_timeout_deadline_cancel,
        profiles=("smoke", "full"),
    )

    # ----------------------------
    # DB adapter contracts
    # ----------------------------
    def t_db_claim_does_not_set_started(ctx):
        """DB claim should move row to PENDING without setting started/deadline fields."""
        rt = getWorkflows(dbName=db_name)
        wid = uuid4()
        created = long(now_ms())
        db.insert_workflow(
            workflow_id=wid,
            workflow_name="tests.db_contract_claim",
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=None,
            priority=0,
            deduplication_id=None,
            application_version=None,
            inputs_obj={"resolved": {}},
            created_ms=created,
            deadline_ms=None,
        )
        tx = db.begin()
        try:
            claimed = db.claim_enqueued(
                settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx
            )
            db.commit(tx)
        finally:
            db.close(tx)
        assert len(claimed) == 1, "expected one claimed row"
        row = db.get_workflow(wid)
        assert row is not None
        assert str(row.get("status")) == settings.STATUS_PENDING
        assert row.get("started_at_epoch_ms") is None, "claim should not set started_at"
        assert row.get("deadline_epoch_ms") is None, "claim should not set deadline"
        db.release_claim(wid)
        db.cancel_workflow(wid, "test_cleanup")

    _register_case(
        cases,
        "db_contract",
        "claim_does_not_set_started",
        t_db_claim_does_not_set_started,
    )

    def t_db_mark_running_sets_started_and_deadline(ctx):
        """mark_running_if_claimed should atomically set RUNNING, started_at, and deadline."""
        rt = getWorkflows(dbName=db_name)
        wid = uuid4()
        created = long(now_ms())
        db.insert_workflow(
            workflow_id=wid,
            workflow_name="tests.db_contract_mark_running",
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=None,
            priority=0,
            deduplication_id=None,
            application_version=None,
            inputs_obj={"resolved": {}},
            created_ms=created,
            deadline_ms=None,
        )
        tx = db.begin()
        try:
            db.claim_enqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
            db.commit(tx)
        finally:
            db.close(tx)

        started_ms = long(now_ms())
        n = db.mark_running_if_claimed(
            workflow_id=wid,
            executor_id=rt.executor_id,
            started_ms=started_ms,
            timeout_ms=2000,
        )
        assert int(n) == 1, "expected mark_running_if_claimed update"
        row = db.get_workflow(wid)
        assert row is not None
        assert str(row.get("status")) == settings.STATUS_RUNNING
        assert long(row.get("started_at_epoch_ms")) == started_ms
        delta = long(row.get("deadline_epoch_ms") - row.get("started_at_epoch_ms"))
        assert delta == 2000, "expected deadline-started delta of 2000ms"
        db.cancel_workflow(wid, "test_cleanup")

    _register_case(
        cases,
        "db_contract",
        "mark_running_sets_started_and_deadline",
        t_db_mark_running_sets_started_and_deadline,
    )

    def t_db_release_claim_clears_claim_fields(ctx):
        """release_claim should clear claim metadata and put row back in ENQUEUED."""
        rt = getWorkflows(dbName=db_name)
        wid = uuid4()
        created = long(now_ms())
        db.insert_workflow(
            workflow_id=wid,
            workflow_name="tests.db_contract_release_claim",
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=None,
            priority=0,
            deduplication_id=None,
            application_version=None,
            inputs_obj={"resolved": {}},
            created_ms=created,
            deadline_ms=None,
        )
        tx = db.begin()
        try:
            db.claim_enqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
            db.commit(tx)
        finally:
            db.close(tx)

        n = db.release_claim(wid)
        assert int(n) == 1
        row = db.get_workflow(wid)
        assert row is not None
        assert str(row.get("status")) == settings.STATUS_ENQUEUED
        assert row.get("claimed_by") is None
        assert row.get("claimed_at_epoch_ms") is None
        assert row.get("started_at_epoch_ms") is None
        assert row.get("deadline_epoch_ms") is None
        db.cancel_workflow(wid, "test_cleanup")

    _register_case(
        cases,
        "db_contract",
        "release_claim_clears_claim_fields",
        t_db_release_claim_clears_claim_fields,
    )

    def t_db_terminal_guard(ctx):
        """update_status_if_not_terminal must not overwrite terminal statuses."""
        wid = uuid4()
        created = long(now_ms())
        db.insert_workflow(
            workflow_id=wid,
            workflow_name="tests.db_contract_terminal_guard",
            queue_name=settings.QUEUE_DEFAULT,
            partition_key=None,
            priority=0,
            deduplication_id=None,
            application_version=None,
            inputs_obj={"resolved": {}},
            created_ms=created,
            deadline_ms=None,
        )
        db.cancel_workflow(wid, "test_terminal_guard")
        n = db.update_status_if_not_terminal(
            workflow_id=wid,
            status=settings.STATUS_SUCCESS,
            fields={"result_json": json.dumps({"bad": True})},
        )
        assert int(n) == 0, "terminal guard should prevent status overwrite"
        row = db.get_workflow(wid)
        assert row is not None
        assert str(row.get("status")) == settings.STATUS_CANCELLED

    _register_case(
        cases, "db_contract", "terminal_guard_blocks_overwrite", t_db_terminal_guard
    )

    # ----------------------------
    # Retention
    # ----------------------------
    def t_retention_apply_smoke(ctx):
        """Retention call should execute without throwing."""
        rt = getWorkflows(dbName=db_name)
        rt.applyRetention()

    _register_case(
        cases,
        "retention",
        "apply_smoke",
        t_retention_apply_smoke,
        profiles=("smoke", "full"),
    )

    # Always try to leave maintenance off before running cases.
    try:
        admin_api.exitMaintenance(dbName=db_name)
    except:
        pass

    # Filter selected cases by profile and suite selectors.
    selected = []
    for case in cases:
        if include_suites is not None and case.get("suite") not in include_suites:
            continue
        if exclude_suites is not None and case.get("suite") in exclude_suites:
            continue
        if profile == "smoke" and "smoke" not in case.get("profiles", ()):
            continue
        selected.append(case)

    results = []
    for case in selected:
        results.append(_run_case(case, db_name, run_started_ms))

    # Always leave maintenance mode off at end.
    try:
        admin_api.exitMaintenance(dbName=db_name)
    except:
        pass

    swept = 0
    try:
        swept = _sweep_stragglers(db_name, run_started_ms)
        if swept:
            log.warn("Straggler sweep cancelled %d active tests.* workflows" % swept)
    except Exception as e:
        log.warn("straggler sweep failed: %s" % e)

    passed = len([r for r in results if r.get("ok")])
    failed = len(results) - passed

    suite_map = {}
    for r in results:
        suite = r.get("suite")
        if suite not in suite_map:
            suite_map[suite] = {
                "suite": suite,
                "passed": 0,
                "failed": 0,
                "total": 0,
                "duration_ms": 0,
            }
        row = suite_map[suite]
        row["total"] += 1
        row["duration_ms"] += int(r.get("duration_ms") or 0)
        if r.get("ok"):
            row["passed"] += 1
        else:
            row["failed"] += 1

    suites = sorted(suite_map.values(), key=lambda s: s.get("suite") or "")

    return {
        "ok": failed == 0,
        "profile": profile,
        "summary": {
            "passed": passed,
            "failed": failed,
            "total": len(results),
            "duration_ms": _now_ms() - started,
            "swept_stragglers": swept,
            "suites": [s.get("suite") for s in suites],
        },
        "suites": suites,
        "cases": results,
    }


# ----------------------------
# Report formatting
# ----------------------------
def format_report_text(report):
    """
    Render report as plain text for Script Console output.

    Args:
            report (dict): Report from `run_all`.

    Returns:
            str: Human-readable plain text summary.
    """
    s = []
    s.append(
        "Workflows Tests (%s): %s"
        % (report.get("profile"), "PASS" if report.get("ok") else "FAIL")
    )
    summ = report.get("summary", {})
    s.append(
        "Passed: %s  Failed: %s  Total: %s  Duration(ms): %s  Swept: %s"
        % (
            summ.get("passed"),
            summ.get("failed"),
            summ.get("total"),
            summ.get("duration_ms"),
            summ.get("swept_stragglers"),
        )
    )
    s.append("")

    if report.get("suites"):
        s.append("Suites:")
        for suite in report.get("suites", []):
            s.append(
                "- %s: %s passed / %s failed (%s ms)"
                % (
                    suite.get("suite"),
                    suite.get("passed"),
                    suite.get("failed"),
                    suite.get("duration_ms"),
                )
            )
        s.append("")

    for c in report.get("cases", []):
        s.append(
            "[%s] %s (%d ms)"
            % (
                "OK" if c.get("ok") else "FAIL",
                c.get("id"),
                int(c.get("duration_ms", 0)),
            )
        )
        if not c.get("ok"):
            s.append(c.get("details") or "")
            s.append("")
    return "\n".join(s)


def format_report_md(report):
    """
    Render report as markdown for Perspective display.

    Args:
            report (dict): Report from `run_all`.

    Returns:
            str: Markdown document with summary, suite table, and failures.
    """
    summ = report.get("summary", {})
    header = "## Workflows Tests (%s) - %s\n" % (
        report.get("profile", "full"),
        "PASSED" if report.get("ok") else "FAILED",
    )
    header += (
        "- Passed: **%s**\n"
        "- Failed: **%s**\n"
        "- Total: **%s**\n"
        "- Duration: **%s ms**\n"
        "- Swept Stragglers: **%s**\n\n"
        % (
            summ.get("passed"),
            summ.get("failed"),
            summ.get("total"),
            summ.get("duration_ms"),
            summ.get("swept_stragglers"),
        )
    )

    lines = [header]

    suites = report.get("suites", [])
    if suites:
        lines.append("### Suites")
        lines.append("| Suite | Passed | Failed | Total | Duration (ms) |")
        lines.append("|---|---:|---:|---:|---:|")
        for s in suites:
            lines.append(
                "| `%s` | %s | %s | %s | %s |"
                % (
                    s.get("suite"),
                    s.get("passed"),
                    s.get("failed"),
                    s.get("total"),
                    s.get("duration_ms"),
                )
            )
        lines.append("")

    lines.append("### Cases")
    lines.append("| Result | Case | Duration (ms) |")
    lines.append("|---|---|---:|")
    for c in report.get("cases", []):
        res = "PASSED" if c.get("ok") else "FAILED"
        lines.append(
            "| %s | `%s` | %d |" % (res, c.get("id"), int(c.get("duration_ms", 0)))
        )
    lines.append("")

    # Slowest cases are handy when optimizing test runtime.
    sorted_cases = sorted(
        report.get("cases", []),
        key=lambda c: int(c.get("duration_ms", 0)),
        reverse=True,
    )
    slowest = sorted_cases[:5]
    if slowest:
        lines.append("### Slowest Cases")
        for c in slowest:
            lines.append("- `%s` - %d ms" % (c.get("id"), int(c.get("duration_ms", 0))))
        lines.append("")

    failures = [c for c in report.get("cases", []) if not c.get("ok")]
    if failures:
        lines.append("### Failures")
        for f in failures:
            lines.append("#### `%s`" % f.get("id"))
            lines.append("```")
            lines.append((f.get("details") or "")[:12000])
            lines.append("```")

    return "\n".join(lines)


def run_all_md(
    db_name="WorkflowsDB", profile="full", include_suites=None, exclude_suites=None
):
    """
    Run suite and return both raw report and markdown output.

    Args:
            db_name (str): Ignition DB connection name.
            profile (str): `full` or `smoke`.
            include_suites (str|list|tuple|set|None): Optional suite allow-list.
            exclude_suites (str|list|tuple|set|None): Optional suite deny-list.

    Returns:
            dict: `{report: <dict>, markdown: <str>}`.
    """
    report = run_all(
        db_name=db_name,
        profile=profile,
        include_suites=include_suites,
        exclude_suites=exclude_suites,
    )
    return {"report": report, "markdown": format_report_md(report)}
