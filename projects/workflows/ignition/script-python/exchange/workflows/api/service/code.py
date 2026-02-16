# exchange/workflows/api/service.py
"""
Workflows service API used by timer scripts, tag scripts, and Perspective actions.

Primary entry points:
- dispatch(): one flush + dispatch cycle.
- start(): enqueue a workflow in durable DB storage.
- enqueueInMemory(): in-memory enqueue for low-latency tag events.
- sendCommand(): mailbox command delivery.
- cancel(): cooperative workflow cancellation request.
- listWorkflows(): workflow selector data for UI.

"""

from exchange.workflows import settings
from exchange.workflows.api import admin as admin_api
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.util import response

log = system.util.getLogger(settings.getLoggerName("api.service"))


def _meta(dbName=None, queueName=None, generation=None):
    """
    Build common metadata fields for service API responses.

    Args:
            dbName (str|None): database connection name.
            queueName (str|None): Queue name when relevant.
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


def _getRuntimeGeneration(rt):
    """
    Read runtime generation safely for response metadata.

    Args:
            rt (object): WorkflowsRuntime facade.

    Returns:
            long|None: Generation value when available.
    """
    try:
        return long(rt._getExecutorGeneration())
    except:
        return None


def dispatch(
    queueName=None, maxToClaim=None, dbName=None, flushMaxItems=None, flushMaxMs=None
):
    """
    Run one gateway dispatch: flush in-memory queue items, then claim/dispatch queued workflows.

    This should be called (tipically) from a the gateway timer script (`handleTimerEvent.py`).

    Args:
            queueName (str|None): Target queue; defaults to `settings.QUEUE_DEFAULT`.
            maxToClaim (int|None): Max ENQUEUED rows to claim; defaults to settings.MAX_CLAIM_DEFAULT.
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.
            flushMaxItems (int|None): Max in-memory queue items to flush before dispatch.
            flushMaxMs (int|None): Flush time budget in milliseconds.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "DISPATCH_OK" | "DISPATCH_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # claimedCount
        "meta": {...}                 # tsMs, durationMs, dbName, queueName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    queueName = settings.resolveQueueName(queueName)
    dbName = settings.resolveDbName(dbName)
    if maxToClaim is None:
        maxToClaim = settings.MAX_CLAIM_DEFAULT
    if flushMaxItems is None:
        flushMaxItems = settings.FAST_FLUSH_MAX_ITEMS
    if flushMaxMs is None:
        flushMaxMs = settings.FAST_FLUSH_MAX_MS

    try:
        rt = getWorkflows(dbName=dbName)
        claimedCount = rt.dispatch(
            queueName=queueName,
            maxToClaim=int(maxToClaim),
            flushMaxItems=int(flushMaxItems),
            flushMaxMs=int(flushMaxMs),
        )
        return response.makeOk(
            code="DISPATCH_OK",
            message="Dispatch completed.",
            data={"claimedCount": claimedCount},
            meta=_meta(
                dbName=dbName,
                queueName=queueName,
                generation=_getRuntimeGeneration(rt),
            ),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="DISPATCH_FAILED",
            message="Dispatch failed.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )


def start(
    workflowName=None,
    inputs=None,
    queueName=None,
    partitionKey=None,
    priority=0,
    deduplicationId=None,
    applicationVersion=None,
    timeoutSeconds=None,
    allowDuringMaintenance=False,
    dbName=None,
):
    """
    Validate and enqueue a workflow as an ENQUEUED row in Postgres.

    Use this from Perspective actions or other gateway scripts when you want
    durable scheduling that can also be picked up by the external CPython worker.

    Args:
            workflowName (str|None): Registered workflow function name.
            inputs (dict|None): Workflow input payload.
            queueName (str|None): Target queue; defaults to `settings.QUEUE_DEFAULT`.
            partitionKey (str|None): Optional partition key for serialized processing.
            priority (int): Priority used during claim ordering.
            deduplicationId (str|None): Optional dedupe id.
            applicationVersion (str|None): Optional app/version stamp.
            timeoutSeconds (float|None): Optional execution timeout.
            allowDuringMaintenance (bool): Allow enqueue while maintenance is enabled.
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "WORKFLOW_ENQUEUED" | "MAINTENANCE_ACTIVE" | "WORKFLOW_NOT_REGISTERED" | "START_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # workflowId, workflowName
        "meta": {...}                 # tsMs, durationMs, dbName, queueName, generation
        }
    """
    startedMs = system.date.toMillis(system.date.now())
    queueName = settings.resolveQueueName(queueName)
    dbName = settings.resolveDbName(dbName)

    if not workflowName:
        return response.makeErr(
            code="INVALID_WORKFLOW_NAME",
            message="workflowName is required.",
            data={},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )

    try:
        rt = getWorkflows(dbName=dbName)
        workflowId = rt.enqueue(
            workflowName=workflowName,
            inputsObj=inputs or {},
            queueName=queueName,
            partitionKey=partitionKey,
            priority=priority,
            deduplicationId=deduplicationId,
            applicationVersion=applicationVersion,
            timeoutSeconds=timeoutSeconds,
            allowDuringMaintenance=allowDuringMaintenance,
        )
        return response.makeOk(
            code="WORKFLOW_ENQUEUED",
            message="Workflow enqueued.",
            data={"workflowId": workflowId, "workflowName": workflowName},
            meta=_meta(
                dbName=dbName,
                queueName=queueName,
                generation=_getRuntimeGeneration(rt),
            ),
            startedMs=startedMs,
        )
    except Exception as e:
        err = str(e)
        if "Maintenance enabled" in err:
            code = "MAINTENANCE_ACTIVE"
            message = "Start rejected because maintenance mode is enabled."
        elif "Workflow not registered" in err:
            code = "WORKFLOW_NOT_REGISTERED"
            message = "Workflow name is not registered in this interpreter."
        else:
            code = "START_FAILED"
            message = "Failed to enqueue workflow."
        return response.makeErr(
            code=code,
            message=message,
            data={"error": err, "workflowName": workflowName},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )


def enqueueInMemory(
    workflowName=None,
    inputs=None,
    queueName=None,
    partitionKey=None,
    priority=0,
    deduplicationId=None,
    applicationVersion=None,
    timeoutSeconds=None,
    allowDuringMaintenance=None,
    dbName=None,
):
    """
    Add an in-memory enqueue request without direct DB I/O.

    Typical caller is a tag-change script where latency matters more.
    Items are flushed to Postgres during `dispatch()`.

    Args:
            workflowName (str|None): Registered workflow function name.
            inputs (dict|None): Workflow input payload.
            queueName (str|None): Target queue; defaults to `settings.QUEUE_DEFAULT`.
            partitionKey (str|None): Optional partition key for serialized processing.
            priority (int): Priority used during claim ordering after flush.
            deduplicationId (str|None): Optional dedupe id.
            applicationVersion (str|None): Optional app/version stamp.
            timeoutSeconds (float|None): Optional execution timeout.
            allowDuringMaintenance (bool|None): Override maintenance rejection behavior.
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "IN_MEMORY_ENQUEUE_ACCEPTED" | "IN_MEMORY_ENQUEUE_REJECTED" | "IN_MEMORY_ENQUEUE_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # accepted, reason, queueDepth, maxDepth (when provided)
        "meta": {...}                 # tsMs, durationMs, dbName, queueName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    queueName = settings.resolveQueueName(queueName)
    dbName = settings.resolveDbName(dbName)

    if not workflowName:
        return response.makeErr(
            code="INVALID_WORKFLOW_NAME",
            message="workflowName is required.",
            data={},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )

    try:
        rt = getWorkflows(dbName=dbName)
        ack = rt.enqueueInMemory(
            workflowName=workflowName,
            inputsObj=inputs or {},
            queueName=queueName,
            partitionKey=partitionKey,
            priority=priority,
            deduplicationId=deduplicationId,
            applicationVersion=applicationVersion,
            timeoutSeconds=timeoutSeconds,
            allowDuringMaintenance=allowDuringMaintenance,
        )
        ack = ack if isinstance(ack, dict) else {}
        if ack.get("accepted") is True:
            return response.makeOk(
                code="IN_MEMORY_ENQUEUE_ACCEPTED",
                message="In-memory enqueue accepted.",
                data=ack,
                meta=_meta(
                    dbName=dbName,
                    queueName=queueName,
                    generation=_getRuntimeGeneration(rt),
                ),
                startedMs=startedMs,
            )
        return response.makeErr(
            code="IN_MEMORY_ENQUEUE_REJECTED",
            message="In-memory enqueue rejected.",
            data=ack,
            meta=_meta(
                dbName=dbName,
                queueName=queueName,
                generation=_getRuntimeGeneration(rt),
            ),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="IN_MEMORY_ENQUEUE_FAILED",
            message="In-memory enqueue failed.",
            data={"error": str(e), "workflowName": workflowName},
            meta=_meta(dbName=dbName, queueName=queueName),
            startedMs=startedMs,
        )


def sendCommand(workflowId=None, cmd=None, reason=None, topic="cmd", dbName=None):
    """
    Send a mailbox command to a workflow instance. You can send a message to workflows running
    in Ignition or if you have CPython workers that works too!

    Args:
            workflowId (str|None): Destination workflow id.
            cmd (str|object|None): Command value, normalized to upper-case string.
            reason (str|None): Optional operator reason.
            topic (str): Mailbox topic.
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "COMMAND_SENT" | "INVALID_ARGUMENTS" | "COMMAND_SEND_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # workflowId, topic, cmd
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }
    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    if not workflowId or cmd is None:
        return response.makeErr(
            code="INVALID_ARGUMENTS",
            message="workflowId and cmd are required.",
            data={"workflowId": workflowId},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
    try:
        rt = getWorkflows(dbName=dbName)
        msg = {"cmd": str(cmd).upper()}
        if reason:
            msg["reason"] = reason
        rt.send(workflowId, msg, topic=topic)
        return response.makeOk(
            code="COMMAND_SENT",
            message="Command sent to workflow mailbox.",
            data={"workflowId": workflowId, "topic": topic, "cmd": msg.get("cmd")},
            meta=_meta(dbName=dbName, generation=_getRuntimeGeneration(rt)),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="COMMAND_SEND_FAILED",
            message="Failed to send command.",
            data={"error": str(e), "workflowId": workflowId},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def cancel(workflowId=None, reason=None, dbName=None):
    """
    Request cooperative cancellation for a workflow.

    Args:
            workflowId (str|None): Workflow id to cancel.
            reason (str|None): Optional human reason recorded in status.
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "CANCEL_REQUESTED" | "INVALID_ARGUMENTS" | "CANCEL_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # workflowId, cancelledCount
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    if not workflowId:
        return response.makeErr(
            code="INVALID_ARGUMENTS",
            message="workflowId is required.",
            data={},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
    try:
        rt = getWorkflows(dbName=dbName)
        cancelledCount = rt.cancelWorkflow(workflowId, reason=reason or "")
        try:
            cancelledCount = int(cancelledCount)
        except:
            cancelledCount = 0
        return response.makeOk(
            code="CANCEL_REQUESTED",
            message="Cancel request submitted.",
            data={"workflowId": workflowId, "cancelledCount": cancelledCount},
            meta=_meta(dbName=dbName, generation=_getRuntimeGeneration(rt)),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="CANCEL_FAILED",
            message="Failed to cancel workflow.",
            data={"error": str(e), "workflowId": workflowId},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )


def listWorkflows(dbName=None):
    """
    Return registered workflows for dropdowns and selector components.

    Args:
            dbName (str|None): database connection name; defaults to `settings.DBNAME_DEFAULT`.

    Returns:
        response (str, dict):
            A dict corresponding to the standard envelope. Example:
        {
        "ok": True|False,
        "code": "SOME_CODE",          # "WORKFLOW_OPTIONS_OK" | "WORKFLOW_OPTIONS_FAILED"
        "message": "summary",         # short, action-oriented
        "data": {...},                # options (list), count (int)
        "meta": {...}                 # tsMs, durationMs, dbName, generation
        }

    """
    startedMs = system.date.toMillis(system.date.now())
    dbName = settings.resolveDbName(dbName)
    try:
        rt = getWorkflows(dbName=dbName)
        names = rt.listWorkflows()
        options = [{"label": name, "value": name} for name in names]
        return response.makeOk(
            code="WORKFLOW_OPTIONS_OK",
            message="Workflow options loaded.",
            data={"options": options, "count": len(options)},
            meta=_meta(dbName=dbName, generation=_getRuntimeGeneration(rt)),
            startedMs=startedMs,
        )
    except Exception as e:
        return response.makeErr(
            code="WORKFLOW_OPTIONS_FAILED",
            message="Failed to list workflow options.",
            data={"error": str(e)},
            meta=_meta(dbName=dbName),
            startedMs=startedMs,
        )
