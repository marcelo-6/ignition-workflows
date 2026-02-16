# exchange/workflows/api/admin.py
"""
Workflows admin API.

Used by Perspective admin screens and maintenance scripts.

Entry points:
- checkSchema()
- applyRetention()
- enterMaintenance()
- exitMaintenance()
- getMaintenanceStatus()
- swapIfDrained()

Every public function returns the standard envelope:
{ok, code, message, data, meta}
"""

from exchange.workflows import settings
from exchange.workflows.engine.db import DB
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.util import response

log = system.util.getLogger(settings.getLoggerName("api.admin"))


def _meta(dbName=None, queueName=None, generation=None):
    """
    Build common metadata fields for admin API responses.

    Args:
            dbName (str|None): database connection name.
            queueName (str|None): Queue name when request targets a specific queue.
            generation (int|long|None): Runtime generation value when available.

    Returns:
            dict: Metadata object for the response envelope.
    """
    meta = {"dbName": dbName}
    if queueName is not None:
        meta["queueName"] = queueName
    if generation is not None:
        meta["generation"] = generation
    return meta


def checkSchema(dbName=None):
    """
    Verify the workflow schema is reachable from the configured database connection.

    Use this from admin pages before running maintenance operations.

    Args:
            dbName (str|None): database connection name. Defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "SCHEMA_OK" | "SCHEMA_CHECK_FAILED"
        "message": "summary",   # short, action-oriented
        "data": {...},                # `schemaVersion` when available
        "meta": {...}                 # tsMs, durationMs, dbName
        }
    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        db = DB(dbName)
        schemaVersion = db.scalar(
            "SELECT value FROM workflows.schema_meta WHERE key='schema_version'", []
        )
        return response.makeOk(
            code="SCHEMA_OK",
            message="Workflow schema is reachable.",
            data={"schemaVersion": schemaVersion},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="SCHEMA_CHECK_FAILED",
            message="Failed to read workflow schema metadata.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def applyRetention(dbName=None):
    """
    Apply retention policies using the runtime retention configuration row.

    Use this from a scheduled maintenance button/script when you want cleanup
    outside of normal tick cadence.

    Args:
            dbName (str|None): database connection name. Defaults to settings.DBNAME_DEFAULT.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "RETENTION_APPLIED" | "RETENTION_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # Empty on success
        "meta": {...}                 # tsMs, durationMs, dbName
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        rt = getWorkflows(dbName=dbName)
        rt.applyRetention()
        return response.makeOk(
            code="RETENTION_APPLIED",
            message="Retention cleanup completed.",
            data={},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
    except Exception as e:
        log.warn("retention failed db=%s err=%s" % (dbName, e))
        return response.makeErr(
            code="RETENTION_FAILED",
            message="Retention cleanup failed.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def enterMaintenance(mode="drain", reason="", queueName=None, dbName=None):
    """
    Enable maintenance mode to prevent mixed-version workflow dispatch.

    In `drain` mode the dispatcher stops claiming new work and lets in-flight work
    finish. In `cancel` mode it also cancels queued/running workflows cooperatively.

    Args:
            mode (str): `drain` or `cancel`.
            reason (str): Operator-provided reason shown in status and logs.
            queueName (str|None): Target queue for cancel operations. Defaults to `settings.QUEUE_DEFAULT`.
            dbName (str|None): database connection name. Defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "MAINTENANCE_ENABLED" | "INVALID_MODE" | "MAINTENANCE_ENABLE_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # runtime maintenance status fields
        "meta": {...}                 # tsMs, durationMs, dbName, queueName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    mode = str(mode or "drain").lower()
    queueName = settings.resolveQueueName(queueName)
    dbName = settings.resolveDbName(dbName)

    if mode not in ("drain", "cancel"):
        return response.makeErr(
            code="INVALID_MODE",
            message="Maintenance mode must be 'drain' or 'cancel'.",
            data={"mode": mode},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )

    try:
        rt = getWorkflows(dbName=dbName)
        status = rt.enterMaintenance(mode=mode, reason=reason, queueName=queueName)
        gen = status.get("generation") if isinstance(status, dict) else None
        return response.makeOk(
            code="MAINTENANCE_ENABLED",
            message="Maintenance mode enabled.",
            data=status if isinstance(status, dict) else {},
            meta=_meta(dbName=dbName, queueName=queueName, generation=gen),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="MAINTENANCE_ENABLE_FAILED",
            message="Failed to enable maintenance mode.",
            data={"error": str(e), "mode": mode, "reason": reason},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )


def exitMaintenance(dbName=None):
    """
    Disable maintenance mode and resume normal dispatch behavior.

    Args:
            dbName (str|None): database connection name. Defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "MAINTENANCE_DISABLED" | "MAINTENANCE_DISABLE_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # runtime maintenance status fields
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        rt = getWorkflows(dbName=dbName)
        status = rt.exitMaintenance()
        gen = status.get("generation") if isinstance(status, dict) else None
        return response.makeOk(
            code="MAINTENANCE_DISABLED",
            message="Maintenance mode disabled.",
            data=status if isinstance(status, dict) else {},
            meta=_meta(dbName=dbName, generation=gen),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="MAINTENANCE_DISABLE_FAILED",
            message="Failed to disable maintenance mode.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def getMaintenanceStatus(dbName=None):
    """
    Read current maintenance and runtime health status.

    Args:
            dbName (str|None): database connection name. Defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "MAINTENANCE_STATUS" | "MAINTENANCE_STATUS_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # maintenance flags, generation, in-flight counters, executor stats
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        rt = getWorkflows(dbName=dbName)
        status = rt.getMaintenanceStatus()
        gen = status.get("generation") if isinstance(status, dict) else None
        return response.makeOk(
            code="MAINTENANCE_STATUS",
            message="Maintenance status retrieved.",
            data=status if isinstance(status, dict) else {},
            meta=_meta(dbName=dbName, generation=gen),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="MAINTENANCE_STATUS_FAILED",
            message="Failed to read maintenance status.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def swapIfDrained(dbName=None):
    """
    Swap runtime executors when the runtime is fully drained.

    This is the deployment cutover action. It only succeeds when maintenance
    is enabled and no workflows are in flight.

    Args:
            dbName (str|None): database connection name. Defaults to settings.DBNAME_DEFAULT.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "MAINTENANCE_SWAP_OK" | "MAINTENANCE_REQUIRED" | "NOT_DRAINED" | "SWAP_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # swap diagnostics including generation and drain counters
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        rt = getWorkflows(dbName=dbName)
        result = rt.swapIfDrained()
        result = result if isinstance(result, dict) else {}
        gen = result.get("generation")
        reason = str(result.get("reason") or "")
        if result.get("ok") is True:
            return response.makeOk(
                code="MAINTENANCE_SWAP_OK",
                message="Runtime executors swapped successfully.",
                data=result,
                meta=_meta(dbName=dbName, generation=gen),
                startedMs=startedMs,
            )
        if reason == "maintenance_required":
            code = "MAINTENANCE_REQUIRED"
            message = "Enable maintenance mode before swapIfDrained."
        elif reason == "not_drained":
            code = "NOT_DRAINED"
            message = "Swap blocked because workflows are still active."
        else:
            code = "SWAP_FAILED"
            message = "Runtime swap was not performed."
        return response.makeErr(
            code=code,
            message=message,
            data=result,
            meta=_meta(dbName=dbName, generation=gen),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="SWAP_FAILED",
            message="Failed to evaluate runtime swap.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
