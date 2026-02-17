# exchange/workflows/tests/support/helpers.py
"""
Shared helpers for the unittest-based workflows test suites.

This module keeps the noisy integration bits in one place so suite files can stay
focused on behavior checks instead of DB polling and cleanup mechanics.
"""

import json
import time
import traceback

from java.util.concurrent import ConcurrentHashMap, ConcurrentLinkedQueue
from java.util.concurrent.atomic import AtomicBoolean, AtomicInteger, AtomicLong

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.db import DB, nowMs
import exchange.workflows.engine.instance as instance_mod
from exchange.workflows.api import admin as admin_api

TEST_WORKFLOW_TIMEOUT_S = 8.0
TEST_CASE_CLEANUP_TICK_S = 2.0
TEST_SWEEP_TICK_S = 3.0

_TERMINAL = tuple(settings.WORKFLOW_TERMINAL_STATUSES)


def nowMs():
    """Return current gateway epoch milliseconds."""
    return system.date.toMillis(system.date.now())


def fmtTs(ms):
    """Format epoch milliseconds into a human-readable timestamp string."""
    try:
        return system.date.format(
            system.date.fromMillis(long(ms)), "yyyy-MM-dd HH:mm:ss.SSS"
        )
    except:
        return ""


def safeLoads(raw):
    """Parse JSON safely and return None when parse fails."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except:
        return None


def assertEnvelope(resp, expectOk=None, expectCode=None):
    """Assert the standard API envelope shape and optional values."""
    assert isinstance(resp, dict), "response must be a dict"
    for key in ("ok", "code", "message", "data", "meta"):
        assert key in resp, "missing envelope key: %s" % key
    assert isinstance(resp.get("ok"), bool), "ok must be bool"
    assert isinstance(resp.get("code"), basestring), "code must be string"
    assert isinstance(resp.get("message"), basestring), "message must be string"
    assert isinstance(resp.get("data"), dict), "data must be dict"
    assert isinstance(resp.get("meta"), dict), "meta must be dict"
    assert "tsMs" in resp.get("meta"), "meta.tsMs must be present"
    if expectOk is not None:
        assert resp.get("ok") is bool(expectOk), "expected ok=%s got=%s" % (
            expectOk,
            resp.get("ok"),
        )
    if expectCode is not None:
        assert resp.get("code") == expectCode, "expected code=%s got=%s" % (
            expectCode,
            resp.get("code"),
        )


def countRows(dbName, sql, args):
    """Run a COUNT(*) query and return the value as long."""
    ds = system.db.runPrepQuery(sql, args, dbName)
    return long(ds.getValueAt(0, 0))


def getStatus(db, workflowId):
    """Read the workflow_status row fields commonly used by tests."""
    ds = system.db.runPrepQuery(
        "SELECT status, result_json, error_json, claimed_by, deadline_epoch_ms, "
        "started_at_epoch_ms, completed_at_epoch_ms "
        "FROM workflows.workflow_status WHERE workflow_id=?::uuid",
        [workflowId],
        db.dbName,
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


def waitTerminal(db, workflowId, timeoutS=10.0):
    """Poll a workflow until it reaches a terminal state or timeout."""
    deadline = nowMs() + int(float(timeoutS) * 1000.0)
    while nowMs() < deadline:
        st = getStatus(db, workflowId)
        if st and st.get("status") in _TERMINAL:
            return st
        time.sleep(0.1)
    return None


def tickFor(dbName, seconds=2.0, queueName=None, maxToClaim=10):
    """Drive runtime dispatch in a short loop for side-effect based tests."""
    rt = getWorkflows(dbName=dbName)
    queueName = settings.resolveQueueName(queueName)
    endMs = nowMs() + int(float(seconds) * 1000.0)
    while nowMs() < endMs:
        rt.dispatch(queueName=queueName, maxToClaim=int(maxToClaim))
        time.sleep(0.1)


def tickUntilTerminal(dbName, workflowId, timeoutS=10.0, queueName=None, maxToClaim=10):
    """Dispatch and poll until one workflow finishes or timeout expires."""
    rt = getWorkflows(dbName=dbName)
    db = DB(dbName)
    queueName = settings.resolveQueueName(queueName)
    deadlineMs = nowMs() + int(float(timeoutS) * 1000.0)
    while nowMs() < deadlineMs:
        rt.dispatch(queueName=queueName, maxToClaim=int(maxToClaim))
        st = getStatus(db, workflowId)
        if st and st.get("status") in _TERMINAL:
            return st
        time.sleep(0.1)
    return None


def latestWorkflowId(dbName, workflowName, createdAfterMs=None):
    """Return the most recent workflow id for a workflow name."""
    sql = "SELECT workflow_id FROM workflows.workflow_status WHERE workflow_name=? "
    args = [workflowName]
    if createdAfterMs is not None:
        sql += "AND created_at_epoch_ms>=? "
        args.append(long(createdAfterMs))
    sql += "ORDER BY created_at_epoch_ms DESC LIMIT 1"
    ds = system.db.runPrepQuery(sql, args, dbName)
    if ds.getRowCount() == 0:
        return None
    return str(ds.getValueAt(0, 0))


def workflowIdsSince(dbName, workflowName, createdAfterMs):
    """List workflow ids for one workflow created after a timestamp."""
    ds = system.db.runPrepQuery(
        "SELECT workflow_id FROM workflows.workflow_status "
        "WHERE workflow_name=? AND created_at_epoch_ms>=? "
        "ORDER BY created_at_epoch_ms ASC",
        [workflowName, long(createdAfterMs)],
        dbName,
    )
    out = []
    for r in range(ds.getRowCount()):
        out.append(str(ds.getValueAt(r, 0)))
    return out


def getOperationOutputs(dbName, workflowId):
    """Read operation_outputs rows for one workflow id."""
    ds = system.db.runPrepQuery(
        "SELECT call_seq, step_name, status, attempts, output_json, error_json "
        "FROM workflows.operation_outputs WHERE workflow_id=?::uuid ORDER BY call_seq ASC",
        [workflowId],
        dbName,
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


def assertEventHistoryContains(dbName, workflowId, key, expectedValue):
    """Assert event history contains one expected value for a key."""
    ds = system.db.runPrepQuery(
        "SELECT value_json FROM workflows.workflow_events_history "
        "WHERE workflow_id=?::uuid AND key=? ORDER BY created_at_epoch_ms ASC",
        [workflowId, key],
        dbName,
    )
    values = []
    for r in range(ds.getRowCount()):
        raw = ds.getValueAt(r, 0)
        parsed = safeLoads(raw)
        values.append(parsed if parsed is not None else raw)

    flat = []
    for value in values:
        if isinstance(value, basestring):
            flat.append(value)
        elif isinstance(value, dict) and "value" in value:
            flat.append(value.get("value"))
    assert (
        expectedValue in values or expectedValue in flat
    ), "Expected %r in key=%s history, got=%r" % (expectedValue, key, flat or values)


def forceReenqueue(dbName, workflowId):
    """Reset one workflow row to ENQUEUED so replay behavior can be tested."""
    now = long(nowMs())
    return system.db.runPrepUpdate(
        "UPDATE workflows.workflow_status "
        "SET status=?, claimed_by=NULL, claimed_at_epoch_ms=NULL, "
        "started_at_epoch_ms=NULL, deadline_epoch_ms=NULL, completed_at_epoch_ms=NULL, "
        "updated_at_epoch_ms=?, heartbeat_at_epoch_ms=? "
        "WHERE workflow_id=?::uuid",
        [settings.STATUS_ENQUEUED, now, now, workflowId],
        dbName,
    )


def countActiveWorkflows(dbName, workflowName=None):
    """Count active workflow rows, optionally filtered to one workflow name."""
    if workflowName is None:
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
        args = [workflowName]
    return countRows(dbName, sql, args)


def getKernelOrNone():
    """Read the persistent workflows kernel directly from gateway store."""
    try:
        store, _storeType = instance_mod._getStoreMap()
        if store is None:
            return None
        return instance_mod._getStore(store, instance_mod._KERNEL_KEY)
    except:
        return None


def parseSuiteFilter(value):
    """Normalize suite filter arguments into a set of suite names."""
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


def cancelIfActive(dbName, workflowId, reason="test_cleanup"):
    """Cancel one workflow only when it is currently non-terminal."""
    rt = getWorkflows(dbName=dbName)
    db = DB(dbName)
    st = getStatus(db, workflowId)
    if st is None:
        return False
    if st.get("status") in _TERMINAL:
        return False
    count = rt.cancelWorkflow(workflowId, reason=reason)
    if count is None or int(count) <= 0:
        return False
    return True


def cleanupCase(dbName, startedWorkflowIds):
    """Best-effort cleanup for workflows started inside one test case."""
    anyCancelled = False
    for workflowId in startedWorkflowIds:
        if cancelIfActive(dbName, workflowId, reason="test_cleanup"):
            anyCancelled = True
    if anyCancelled:
        tickFor(dbName, seconds=TEST_CASE_CLEANUP_TICK_S)


def sweepStragglers(dbName, runStartedMs):
    """Cancel active tests.* workflows created during this run."""
    rt = getWorkflows(dbName=dbName)
    ds = system.db.runPrepQuery(
        "SELECT workflow_id FROM workflows.workflow_status "
        "WHERE workflow_name LIKE 'tests.%' "
        "AND status IN ('ENQUEUED','PENDING','RUNNING') "
        "AND created_at_epoch_ms>=?",
        [long(runStartedMs)],
        dbName,
    )
    ids = []
    for r in range(ds.getRowCount()):
        ids.append(str(ds.getValueAt(r, 0)))
    if not ids:
        return 0
    for workflowId in ids:
        rt.cancelWorkflow(workflowId, reason="test_straggler_sweep")
    tickFor(dbName, seconds=TEST_SWEEP_TICK_S)
    return len(ids)


def profiles(*names):
    """Attach profile tags to a unittest test method."""
    cleaned = []
    for name in names:
        value = str(name or "").strip().lower()
        if value:
            cleaned.append(value)
    if not cleaned:
        cleaned = ["full"]

    def _wrap(fn):
        fn._profiles = tuple(cleaned)
        return fn

    return _wrap


def getTestProfiles(method):
    """Return profile tags for one test method."""
    profiles_value = getattr(method, "_profiles", None)
    if profiles_value is None:
        return ("full",)
    out = []
    for value in profiles_value:
        name = str(value or "").strip().lower()
        if name:
            out.append(name)
    if not out:
        return ("full",)
    return tuple(out)


def formatErr(err):
    """Convert unittest error tuple into readable traceback text."""
    try:
        return "".join(traceback.format_exception(err[0], err[1], err[2]))
    except:
        try:
            return str(err)
        except:
            return "<unprintable error>"


def resetRuntimeForTestRun(dbName):
    """Put runtime in a clean baseline state before or after a run."""
    try:
        admin_api.exitMaintenance(dbName=dbName)
    except:
        pass
