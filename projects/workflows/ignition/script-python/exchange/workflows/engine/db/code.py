# exchange/workflows/engine/db.py
"""
Database adapter and SQL helpers for workflows runtime.

Use this module for engine-internal SQL access only. Perspective code should
use named queries where appropriate.

Logging goals:
- Make DB behavior observable in Ignition Gateway logs
- Record execution time + rowcounts
- Capture enough context to debug failures (SQL + args preview)

Timestamp semantics:
- created_at_epoch_ms: enqueue time
- claimed_at_epoch_ms: claim ownership/lease time
- started_at_epoch_ms: actual user-code start time
- deadline_epoch_ms: derived from started_at_epoch_ms + timeout (not claim time)
"""

import json
import traceback
import time
from java.util import UUID

from exchange.workflows import settings
from exchange.workflows.util.date import nowMs

log = system.util.getLogger(settings.getLoggerName("engine.db"))

_WORKFLOW_TERMINAL = tuple(settings.WORKFLOW_TERMINAL_STATUSES)
_WORKFLOW_TERMINAL_FOR_DELETE = (
    settings.STATUS_SUCCESS,
    settings.STATUS_ERROR,
    settings.STATUS_CANCELLED,
)
_DELETE_WORKFLOW_CHILD_SQL = (
    "DELETE FROM workflows.operation_outputs WHERE workflow_id = ?::uuid",
    "DELETE FROM workflows.workflow_events WHERE workflow_id = ?::uuid",
    "DELETE FROM workflows.workflow_events_history WHERE workflow_id = ?::uuid",
    "DELETE FROM workflows.notifications WHERE destination_uuid = ?::uuid",
    "DELETE FROM workflows.streams WHERE workflow_id = ?::uuid",
    "DELETE FROM workflows.stream_heads WHERE workflow_id = ?::uuid",
    "DELETE FROM workflows.workflow_status WHERE workflow_id = ?::uuid",
)

# ----------------------------
# Logging controls (runtime configurable)
# ----------------------------

_DEBUG_FLAG_KEY = "exchange.workflows.db.debug"  # bool
_SLOW_MS_KEY = "exchange.workflows.db.slow_ms"  # int
_LOG_ARGS_KEY = "exchange.workflows.db.log_args"  # bool

_DEFAULT_SLOW_MS = 2000
_DEFAULT_LOG_ARGS = True


def uuid4():
    """Generate a UUID string for workflow and message identifiers."""
    return str(UUID.randomUUID())


def _dumps(obj):
    """Serialize an object to compact JSON for DB."""
    # TODO use system.util.jsonDecode instead?
    if obj is None:
        return None
    return json.dumps(obj, separators=(",", ":"))


def _loads(s):
    """Deserialize JSON text from DB columns into Python objects."""
    # TODO use system.util.jsonEncode instead?
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except:
        return None


def _getStore():
    """Return global settings store used for DB debug flags."""
    # TODO reuse exchange.workflows.engine.instance._getStore?
    try:
        if hasattr(system.util, "globalVarMap"):
            store = system.util.globalVarMap(_DEBUG_FLAG_KEY)
            if store is not None:
                return store
    except:
        pass
    try:
        return system.util.getGlobals()
    except:
        return None


def _getCfgBool(key, default=False):
    """Read a boolean config value from global store."""
    store = _getStore()
    if store is None:
        return default
    try:
        # dict-like
        v = store.get(key, default) if hasattr(store, "get") else store[key]
    except:
        try:
            v = store.get(key) if hasattr(store, "get") else None
        except:
            v = None
    if v is None:
        return default
    try:
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y", "on")
    except:
        return default


def _getCfgInt(key, default=0):
    """Read an integer config value from global store."""
    store = _getStore()
    if store is None:
        return default
    try:
        v = store.get(key, default) if hasattr(store, "get") else store[key]
    except:
        v = default
    try:
        return int(v)
    except:
        return default


def _isDebug():
    """Return whether verbose DB debug logging is enabled."""
    return True  # _getCfgBool(_DEBUG_FLAG_KEY, default=False)


def _slowMs():
    """Return slow-query threshold used for warning logs."""
    return _getCfgInt(_SLOW_MS_KEY, default=_DEFAULT_SLOW_MS)


def _isLogArgsEnabled():
    """Return whether SQL argument previews should be logged."""
    return True  # _getCfgBool(_LOG_ARGS_KEY, default=_DEFAULT_LOG_ARGS)


def _truncate(s, max_len=300):
    """Truncate large values for logs."""
    try:
        if s is None:
            return None
        s2 = str(s)
        if len(s2) <= int(max_len):
            return s2
        return s2[: int(max_len)] + "...(trunc)"
    except:
        return "<unprintable>"


def _getSafeArg(v):
    """
    Prevent logs from being too long:
    - truncate long strings/json
    - summarize dict/list
    """
    try:
        if v is None:
            return None
        if isinstance(v, (int, long, float, bool)):
            return v
        if isinstance(v, (dict, list)):
            try:
                # compact summary
                return _truncate(json.dumps(v, separators=(",", ":")), 300)
            except:
                return _truncate(v, 300)
        return _truncate(v, 300)
    except:
        return "<arg:unprintable>"


def _argsPreview(args):
    """Build a compact argument preview string for query/update logs."""
    if not _isLogArgsEnabled():
        return "args=<suppressed>"
    try:
        if args is None:
            return "args=[]"
        out = []
        for a in list(args):
            out.append(_getSafeArg(a))
        return "args=%s" % _truncate(out, 500)
    except:
        return "args=<error>"


def _sqlPreview(sql):
    """Normalize SQL text for compact single-line logging."""
    try:
        s = " ".join(str(sql).split())
        return _truncate(s, 600)
    except:
        return "<sql:unprintable>"


def _txId(tx):
    """Build a transaction identifier for logs."""
    try:
        if tx is None:
            return "-"
        # tx is a string in Ignition (transaction id)
        return str(tx)
    except:
        return "<tx>"


def _elapsedMs(t0_ms):
    """Compute elapsed milliseconds since a start timestamp."""
    try:
        return long(nowMs() - long(t0_ms))
    except:
        return -1


# ----------------------------
# DB adapter
# ----------------------------


class DB(object):
    """
    DB adapter around system.db.* for Postgres.

    Args:
        dbName (str): database connection name.
    """

    def __init__(self, dbName):
        self.dbName = dbName

    # ---------- transaction helpers ----------

    def _logException(self, event, message):
        log.error("evt=%s %s" % (event, message))
        log.error("evt=%s.trace trace=%s" % (event, traceback.format_exc()))

    def begin(self):
        """
        Start a database transaction.

        Returns:
            id (str): transaction id.
        """
        t0 = nowMs()
        try:
            tx = system.db.beginTransaction(self.dbName)
            return tx
        except Exception as e:
            self._logException(
                "db.tx.begin.error",
                "db=%s ms=%d err=%s" % (self.dbName, _elapsedMs(t0), e),
            )
            raise

    def commit(self, tx):
        """
        Commit a database transaction.

        Args:
            tx (str): transaction id.

        Returns:
            None: Commits the transaction in place.
        """
        t0 = nowMs()
        try:
            system.db.commitTransaction(tx)
        except Exception as e:
            self._logException(
                "db.tx.commit.error",
                "db=%s tx=%s ms=%d err=%s"
                % (self.dbName, _txId(tx), _elapsedMs(t0), e),
            )
            raise

    def rollback(self, tx):
        """
        Roll back a database transaction.

        Args:
            tx (str): transaction id.

        Returns:
            None: Rolls back the transaction in place.
        """
        t0 = nowMs()
        try:
            system.db.rollbackTransaction(tx)
            dt = _elapsedMs(t0)
            log.warn(
                "evt=db.tx.rollback db=%s tx=%s ms=%d" % (self.dbName, _txId(tx), dt)
            )
        except Exception as e:
            self._logException(
                "db.tx.rollback.error",
                "db=%s tx=%s ms=%d err=%s"
                % (self.dbName, _txId(tx), _elapsedMs(t0), e),
            )
            # don't raise rollback errors by default

    def close(self, tx):
        """
        Close a database transaction.

        Args:
            tx (str): transaction id.

        Returns:
            None: Closes transaction resources.
        """
        t0 = nowMs()
        try:
            system.db.closeTransaction(tx)
            dt = _elapsedMs(t0)
            if _isDebug():
                log.debug(
                    "evt=db.tx.close db=%s tx=%s ms=%d" % (self.dbName, _txId(tx), dt)
                )
        except:
            if _isDebug():
                self._logException(
                    "db.tx.close.error",
                    "db=%s tx=%s ms=%d" % (self.dbName, _txId(tx), _elapsedMs(t0)),
                )

    # ---------- core query/update wrappers with logging ----------

    def query(self, sql, args=None, tx=None):
        """
        Execute a prepared query with timing and context logging.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (str|None): Optional transaction id.

        Returns:
            PyDataSet: Query result dataset.
        """
        args = args or []
        t0 = nowMs()
        try:
            if tx is None:
                ds = system.db.runPrepQuery(sql, args, self.dbName)
            else:
                ds = system.db.runPrepQuery(sql, args, self.dbName, tx)

            dt = _elapsedMs(t0)
            rows = -1
            try:
                rows = ds.getRowCount() if ds is not None else 0
            except:
                rows = -1

            if dt >= _slowMs():
                log.warn(
                    "evt=db.q.slow db=%s tx=%s ms=%d rows=%s sql=%s %s"
                    % (
                        self.dbName,
                        _txId(tx),
                        dt,
                        rows,
                        _sqlPreview(sql),
                        _argsPreview(args),
                    )
                )
            elif _isDebug():
                log.debug(
                    "evt=db.q db=%s tx=%s ms=%d rows=%s sql=%s %s"
                    % (
                        self.dbName,
                        _txId(tx),
                        dt,
                        rows,
                        _sqlPreview(sql),
                        _argsPreview(args),
                    )
                )
            return ds
        except Exception as e:
            self._logException(
                "db.q.error",
                "db=%s tx=%s ms=%d err=%s sql=%s %s"
                % (
                    self.dbName,
                    _txId(tx),
                    _elapsedMs(t0),
                    e,
                    _sqlPreview(sql),
                    _argsPreview(args),
                ),
            )
            raise

    def update(self, sql, args=None, tx=None):
        """
        Execute a prepared update with timing and context logging.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (str|None): Optional transaction handle.

        Returns:
            int: Number of affected rows.
        """
        args = args or []
        t0 = nowMs()

        try:
            if tx is None:
                n = system.db.runPrepUpdate(sql, args, self.dbName)
            else:
                n = system.db.runPrepUpdate(sql, args, self.dbName, tx)

            dt = _elapsedMs(t0)
            if dt >= _slowMs():
                log.warn(
                    "evt=db.u.slow db=%s tx=%s ms=%d n=%s sql=%s %s"
                    % (
                        self.dbName,
                        _txId(tx),
                        dt,
                        n,
                        _sqlPreview(sql),
                        _argsPreview(args),
                    )
                )
            elif _isDebug():
                log.debug(
                    "evt=db.u db=%s tx=%s ms=%d n=%s sql=%s %s"
                    % (
                        self.dbName,
                        _txId(tx),
                        dt,
                        n,
                        _sqlPreview(sql),
                        _argsPreview(args),
                    )
                )
            return n
        except Exception as e:
            self._logException(
                "db.u.error",
                "db=%s tx=%s ms=%d err=%s sql=%s %s"
                % (
                    self.dbName,
                    _txId(tx),
                    _elapsedMs(t0),
                    e,
                    _sqlPreview(sql),
                    _argsPreview(args),
                ),
            )
            raise

    def scalar(self, sql, args=None, tx=None, default=None):
        """
        Run a query and return the first column of the first row.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (str|None): Optional transaction handle.
            default (object): Value returned when the result set is empty.

        Returns:
            object: Scalar query result or `default`.
        """
        t0 = nowMs()
        try:
            ds = self.query(sql, args=args, tx=tx)
            if ds is None or ds.getRowCount() == 0:
                if _isDebug():
                    log.debug(
                        "evt=db.scalar.miss db=%s tx=%s ms=%d sql=%s %s"
                        % (
                            self.dbName,
                            _txId(tx),
                            _elapsedMs(t0),
                            _sqlPreview(sql),
                            _argsPreview(args),
                        )
                    )
                return default
            v = ds.getValueAt(0, 0)
            if _isDebug():
                log.debug(
                    "evt=db.scalar.hit db=%s tx=%s ms=%d val=%s sql=%s %s"
                    % (
                        self.dbName,
                        _txId(tx),
                        _elapsedMs(t0),
                        _truncate(v, 200),
                        _sqlPreview(sql),
                        _argsPreview(args),
                    )
                )
            return v
        except:
            # query() already logs errors
            raise

    # ---------- workflow status ----------

    def insertWorkflow(
        self,
        workflowId,
        workflowName,
        queueName,
        partitionKey,
        priority,
        deduplicationId,
        applicationVersion,
        inputsObj,
        createdMs,
        deadlineMs=None,
        tx=None,
    ):
        """
        Insert one ENQUEUED workflow row.

        Args:
            workflowId (str): Workflow UUID.
            workflowName (str): Registered workflow name.
            queueName (str): Queue routing name.
            partitionKey (str|None): Optional partition arbitration key.
            priority (int): Queue priority.
            deduplicationId (str|None): Optional dedupe key.
            applicationVersion (str|None): Optional app/version stamp.
            inputsObj (dict|None): Serialized workflow inputs.
            createdMs (int|long): Created timestamp in epoch ms.
            deadlineMs (int|long|None): Optional deadline timestamp.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of inserted rows.
        """
        sql = """
        INSERT INTO workflows.workflow_status(
          workflow_id, workflow_name, status, queue_name, partition_key, priority,
          deduplication_id, application_version, inputs_json,
          created_at_epoch_ms, updated_at_epoch_ms, deadline_epoch_ms
        )
        VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        n = self.update(
            sql,
            [
                workflowId,
                workflowName,
                settings.STATUS_ENQUEUED,
                queueName,
                partitionKey,
                int(priority),
                deduplicationId,
                applicationVersion,
                _dumps(inputsObj),
                createdMs,
                createdMs,
                deadlineMs if deadlineMs is not None else None,
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.workflow.insert wid=%s wf=%s n=%s"
                % (workflowId, workflowName, n)
            )
        return n

    def insertWorkflowsBatch(self, rows, tx=None):
        """
        Insert multiple workflow rows in a single transaction.

        Args:
            rows (list[dict]): Row payloads matching `insertWorkflow` arguments.
            tx (str|None): Optional transaction handle.

        Returns:
            int: Number of rows inserted.
        """
        if not rows:
            return 0
        inserted = 0
        for row in rows:
            self.insertWorkflow(
                workflowId=row.get("workflow_id"),
                workflowName=row.get("workflow_name"),
                queueName=row.get("queue_name"),
                partitionKey=row.get("partition_key"),
                priority=int(row.get("priority") or 0),
                deduplicationId=row.get("deduplication_id"),
                applicationVersion=row.get("application_version"),
                inputsObj=row.get("inputs_obj"),
                createdMs=long(row.get("created_ms") or nowMs()),
                deadlineMs=row.get("deadline_ms"),
                tx=tx,
            )
            inserted += 1
        return inserted

    def getWorkflow(self, workflowId, tx=None):
        """
        Fetch one workflow_status row by workflow id.

        Args:
            workflowId (str): Workflow UUID.
            tx (str|None): Optional transaction id.

        Returns:
            dict|None: Workflow row dict, or `None` when not found.
        """
        sql = "SELECT * FROM workflows.workflow_status WHERE workflow_id = ?::uuid"
        ds = self.query(sql, [workflowId], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            if _isDebug():
                log.debug("evt=db.workflow.get.miss wid=%s" % workflowId)
            return None
        row = self._rowToDict(ds, 0)
        if _isDebug():
            log.debug(
                "evt=db.workflow.get.hit wid=%s status=%s"
                % (workflowId, row.get("status"))
            )
        return row

    def updateWorkflowStatus(self, workflowId, status, fields=None, tx=None):
        """
        Update workflow status plus optional additional columns.

        Args:
            workflowId (str): Workflow UUID.
            status (str): New workflow status value.
            fields (dict|None): Additional columns to update.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of updated rows.
        """
        fields = fields or {}
        sets = ["status = ?", "updated_at_epoch_ms = ?"]
        args = [status, nowMs()]
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.append(workflowId)
        sql = "UPDATE workflows.workflow_status SET %s WHERE workflow_id = ?::uuid" % (
            ", ".join(sets)
        )
        n = self.update(sql, args, tx=tx)
        if _isDebug():
            log.debug(
                "evt=db.workflow.updateWorkflowStatus wid=%s status=%s n=%s fields=%s"
                % (workflowId, status, n, _truncate(fields, 400))
            )
        return n

    def updateWorkflowStatusIfNotTerminal(
        self, workflowId, status, fields=None, tx=None
    ):
        """
        Update workflow status only when current status is not terminal.

        Args:
            workflowId (str): Workflow UUID.
            status (str): New workflow status value.
            fields (dict|None): Additional columns to update.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of updated rows (0 when already terminal).
        """
        fields = fields or {}
        sets = ["status = ?", "updated_at_epoch_ms = ?"]
        args = [status, nowMs()]
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.append(workflowId)
        args.extend(list(_WORKFLOW_TERMINAL))
        sql = (
            "UPDATE workflows.workflow_status SET %s "
            "WHERE workflow_id = ?::uuid "
            "AND status NOT IN (?, ?, ?)"
        ) % (", ".join(sets))
        n = self.update(sql, args, tx=tx)
        if _isDebug():
            log.debug(
                "evt=db.workflow.updateWorkflowStatusIfNotTerminal wid=%s status=%s n=%s fields=%s"
                % (workflowId, status, n, _truncate(fields, 400))
            )
        return n

    def markRunningIfClaimed(
        self,
        workflowId,
        executorId,
        startedMs=None,
        timeoutMs=None,
        fields=None,
        tx=None,
    ):
        """
        Transition PENDING workflow to RUNNING only when claimed by this executor.

        This is the point where execution officially starts. It sets:
        - started_at_epoch_ms = COALESCE(started_at_epoch_ms, started_ms)
        - deadline_epoch_ms = COALESCE(deadline_epoch_ms, started_ms + timeout_ms)

        Args:
            workflowId (str): Workflow UUID.
            executorId (str): Executor id expected in `claimed_by`.
            startedMs (int|long|None): Execution start timestamp; defaults to `now_ms()`.
            timeoutMs (int|long|None): Timeout duration in ms to derive deadline at start.
            fields (dict|None): Additional fields to update on success.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of updated rows.
        """
        fields = fields or {}
        if startedMs is None:
            startedMs = nowMs()
        else:
            startedMs = startedMs
        sets = [
            "status = ?",
            "updated_at_epoch_ms = ?",
            "started_at_epoch_ms = COALESCE(started_at_epoch_ms, ?)",
            "heartbeat_at_epoch_ms = ?",
        ]
        args = [settings.STATUS_RUNNING, startedMs, startedMs, startedMs]
        if timeoutMs is not None:
            timeoutMs = long(timeoutMs)
            if timeoutMs > 0:
                sets.append("deadline_epoch_ms = COALESCE(deadline_epoch_ms, ?)")
                args.append(long(startedMs + timeoutMs))
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.extend([workflowId, settings.STATUS_PENDING, executorId])
        sql = (
            "UPDATE workflows.workflow_status SET %s "
            "WHERE workflow_id = ?::uuid "
            "AND status = ? "
            "AND claimed_by = ?"
        ) % (", ".join(sets))
        n = self.update(sql, args, tx=tx)
        if _isDebug():
            log.debug(
                "evt=db.workflow.markRunningIfClaimed wid=%s exec=%s n=%s"
                % (workflowId, executorId, n)
            )
        return n

    def releaseClaim(self, workflowId, tx=None):
        """
        Release a PENDING claim back to ENQUEUED.

        Args:
            workflowId (str): Workflow UUID.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of updated rows.
        """
        sql = """
        UPDATE workflows.workflow_status
        SET status = ?,
            claimed_by = NULL,
            claimed_at_epoch_ms = NULL,
            started_at_epoch_ms = NULL,
            deadline_epoch_ms = NULL,
            updated_at_epoch_ms = ?
        WHERE workflow_id = ?::uuid
          AND status = ?
        """
        ms = nowMs()
        n = self.update(
            sql,
            [settings.STATUS_ENQUEUED, ms, workflowId, settings.STATUS_PENDING],
            tx=tx,
        )
        if _isDebug():
            log.debug("evt=db.workflow.releaseClaim wid=%s n=%s" % (workflowId, n))
        return n

    def cancelExpired(self, tx=None):
        """
        Cancel workflows whose deadline has expired.

        Args:
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of cancelled rows.
        """
        return self._cancelWhere(
            "deadline_epoch_ms IS NOT NULL AND deadline_epoch_ms < ?",
            [nowMs()],
            "Timeout exceeded",
            cancelType="timeout",
            tx=tx,
        )

    def cancelWorkflow(self, workflowId, reason, tx=None):
        """
        Cooperatively cancel one workflow (workflow code should poll/check).

        Args:
            workflowId (str): Workflow UUID.
            reason (str): Human-readable cancellation reason.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows updated.
        """
        err = _dumps({"type": "cancelled", "reason": reason})
        sql = """
        UPDATE workflows.workflow_status
        SET status = ?,
            error_json = ?,
            updated_at_epoch_ms = ?,
            completed_at_epoch_ms = COALESCE(completed_at_epoch_ms, ?)
        WHERE workflow_id = ?::uuid
          AND status IN (?, ?, ?)
        """
        ms = nowMs()
        n = self.update(
            sql,
            [
                settings.STATUS_CANCELLED,
                err,
                ms,
                ms,
                workflowId,
                settings.STATUS_ENQUEUED,
                settings.STATUS_PENDING,
                settings.STATUS_RUNNING,
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.workflow.cancel wid=%s n=%s reason=%s"
                % (workflowId, n, _truncate(reason, 200))
            )
        return n

    def _cancelWhere(
        self, whereSql, whereArgs, reason, cancelType="cancelled", tx=None
    ):
        """
        Cancel workflows matching a WHERE clause.

        Args:
            whereSql (str): SQL predicate fragment without `WHERE`.
            whereArgs (list): Prepared-statement values for the predicate.
            reason (str): Human-readable cancellation reason.
            cancelType (str): Error payload cancellation type.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows updated.
        """
        err = _dumps({"type": cancelType, "reason": reason})
        ms = nowMs()
        sql = (
            """
        UPDATE workflows.workflow_status
        SET status = ?,
            error_json = COALESCE(error_json, ?),
            updated_at_epoch_ms = ?,
            completed_at_epoch_ms = COALESCE(completed_at_epoch_ms, ?)
        WHERE %s
        """
            % whereSql
        )
        args = [settings.STATUS_CANCELLED, err, ms, ms] + list(whereArgs)
        n = self.update(sql, args, tx=tx)
        if _isDebug():
            log.debug(
                "evt=db.workflow._cancelWhere n=%s type=%s reason=%s where=%s"
                % (n, cancelType, _truncate(reason, 200), _truncate(whereSql, 200))
            )
        return n

    def cancelEnqueuedPending(self, queueName=None, reason="", tx=None):
        """
        Cancel ENQUEUED and PENDING workflows, optionally scoped to one queue.

        Args:
            queueName (str|None): Queue filter.
            reason (str): Human-readable cancellation reason.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows updated.
        """
        whereSql = "status IN (?, ?)"
        whereArgs = [settings.STATUS_ENQUEUED, settings.STATUS_PENDING]
        if queueName is not None:
            whereSql += " AND queue_name = ?"
            whereArgs.append(queueName)
        return self._cancelWhere(
            whereSql,
            whereArgs,
            reason or "maintenance_cancel",
            cancelType="maintenance_cancel",
            tx=tx,
        )

    def cancelRunning(self, queueName=None, reason="", tx=None):
        """
        Cancel RUNNING workflows, optionally scoped to one queue.

        Args:
            queueName (str|None): Queue filter.
            reason (str): Human-readable cancellation reason.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows updated.
        """
        whereSql = "status = ?"
        whereArgs = [settings.STATUS_RUNNING]
        if queueName is not None:
            whereSql += " AND queue_name = ?"
            whereArgs.append(queueName)
        return self._cancelWhere(
            whereSql,
            whereArgs,
            reason or "maintenance_cancel",
            cancelType="maintenance_cancel",
            tx=tx,
        )

    # ---------- claiming (queue tick) ----------

    def claimEnqueued(self, queueName, maxToClaim, executorId, tx):
        """
        Claim up to N ENQUEUED items for the given queue, using row locks.

        Partition arbitration is DB-level:
        - if partition_key is not null, exclude those partitions that already have PENDING/RUNNING.
        - claim transition only sets PENDING + claim metadata (not started_at/deadline)

        Args:
            queueName (str): Queue to claim from.
            maxToClaim (int): Max number of rows to claim.
            executorId (str): Executor identifier persisted in claim metadata.
            tx (str): Transaction id (required).

        Returns:
            list[dict]: Claimed workflow rows.
        """
        sql = """
        SELECT ws.workflow_id
        FROM workflows.workflow_status ws
        WHERE ws.queue_name = ?
          AND ws.status = ?
          AND (
            ws.partition_key IS NULL OR NOT EXISTS (
              SELECT 1 FROM workflows.workflow_status ws2
              WHERE ws2.queue_name = ws.queue_name
                AND ws2.partition_key = ws.partition_key
                AND ws2.status IN (?, ?)
            )
          )
        ORDER BY ws.priority ASC, ws.created_at_epoch_ms ASC
        FOR UPDATE SKIP LOCKED
        LIMIT ?
        """
        ids_ds = self.query(
            sql,
            [
                queueName,
                settings.STATUS_ENQUEUED,
                settings.STATUS_PENDING,
                settings.STATUS_RUNNING,
                int(maxToClaim),
            ],
            tx=tx,
        )
        ids = []
        for r in range(ids_ds.getRowCount()):
            ids.append(str(ids_ds.getValueAt(r, 0)))

        if _isDebug():
            log.debug(
                "evt=db.claim.ids queue=%s want=%s got=%s exec=%s"
                % (queueName, int(maxToClaim), len(ids), executorId)
            )

        claimed = []
        ms = nowMs()
        for wid in ids:
            upd = """
            UPDATE workflows.workflow_status
            SET status = ?,
                claimed_by = ?,
                claimed_at_epoch_ms = ?,
                updated_at_epoch_ms = ?,
                heartbeat_at_epoch_ms = ?
            WHERE workflow_id = ?::uuid
              AND status = ?
            """
            n = self.update(
                upd,
                [
                    settings.STATUS_PENDING,
                    executorId,
                    ms,
                    ms,
                    ms,
                    wid,
                    settings.STATUS_ENQUEUED,
                ],
                tx=tx,
            )
            if n == 1:
                row = self.getWorkflow(wid, tx=tx)
                if row:
                    claimed.append(row)

        if _isDebug():
            log.debug(
                "evt=db.claim.done queue=%s claimed=%s" % (queueName, len(claimed))
            )
        return claimed

    # ---------- step outputs (durable replay) ----------

    def getStepOutput(self, workflowId, callSeq, tx=None):
        """
        Read step output/error row.

        Args:
            workflowId (str): Workflow UUID.
            callSeq (int): Deterministic step call sequence.
            tx (str|None): Optional transaction id.

        Returns:
            dict|None: Step record, or `None` when not found.
        """
        sql = """
        SELECT status, output_json, error_json, attempts
        FROM workflows.operation_outputs
        WHERE workflow_id = ?::uuid AND call_seq = ?
        """
        ds = self.query(sql, [workflowId, int(callSeq)], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            return None
        return {
            "status": ds.getValueAt(0, 0),
            "output": _loads(ds.getValueAt(0, 1)),
            "error": _loads(ds.getValueAt(0, 2)),
            "attempts": int(ds.getValueAt(0, 3)),
        }

    def insertStepAttempt(self, workflowId, callSeq, stepName, startedMs, tx=None):
        """
        Insert initial step attempt row when it does not already exist.

        Args:
            workflowId (str): Workflow UUID.
            callSeq (int): Deterministic step call sequence.
            stepName (str): Step registry name.
            startedMs (int|long): Step start timestamp.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of inserted rows.
        """
        sql = """
        INSERT INTO workflows.operation_outputs(
          workflow_id, call_seq, step_name, status,
          started_at_epoch_ms, attempts
        )
        VALUES (?::uuid, ?, ?, ?, ?, 1)
        ON CONFLICT (workflow_id, call_seq) DO NOTHING
        """
        n = self.update(
            sql,
            [
                workflowId,
                int(callSeq),
                stepName,
                settings.STEP_STATUS_STARTED,
                long(startedMs),
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.step.insert_attempt wid=%s call_seq=%s step=%s n=%s"
                % (workflowId, int(callSeq), stepName, n)
            )
        return n

    def updateStepSuccess(
        self, workflowId, callSeq, outputObj, completedMs, attempts, tx=None
    ):
        """
        Mark a step attempt as SUCCESS and persist output JSON.

        Args:
            workflowId (str): Workflow UUID.
            callSeq (int): Deterministic step call sequence.
            outputObj (object): Step return payload.
            completedMs (int|long): Completion timestamp.
            attempts (int): Attempt count used for this success.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of updated rows.
        """
        sql = """
        UPDATE workflows.operation_outputs
        SET status = ?,
            output_json = ?,
            error_json = NULL,
            completed_at_epoch_ms = ?,
            attempts = ?
        WHERE workflow_id = ?::uuid AND call_seq = ?
        """
        n = self.update(
            sql,
            [
                settings.STATUS_SUCCESS,
                _dumps(outputObj),
                long(completedMs),
                int(attempts),
                workflowId,
                int(callSeq),
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.step.success wid=%s call_seq=%s attempts=%s n=%s"
                % (workflowId, int(callSeq), int(attempts), n)
            )
        return n

    def updateStepError(
        self, workflowId, callSeq, errorObj, completedMs, attempts, tx=None
    ):
        """
        Mark a step attempt as ERROR and persist error JSON.

        Args:
            workflowId (str): Workflow UUID.
            callSeq (int): Deterministic step call sequence.
            errorObj (dict): Error payload (message/traceback/attempt info).
            completedMs (int|long): Completion timestamp.
            attempts (int): Attempt count reached.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of updated rows.
        """
        sql = """
        UPDATE workflows.operation_outputs
        SET status = ?,
            error_json = ?,
            completed_at_epoch_ms = ?,
            attempts = ?
        WHERE workflow_id = ?::uuid AND call_seq = ?
        """
        n = self.update(
            sql,
            [
                settings.STATUS_ERROR,
                _dumps(errorObj),
                long(completedMs),
                int(attempts),
                workflowId,
                int(callSeq),
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.step.error wid=%s call_seq=%s attempts=%s n=%s err=%s"
                % (
                    workflowId,
                    int(callSeq),
                    int(attempts),
                    n,
                    _truncate(errorObj, 300),
                )
            )
        return n

    # ---------- notifications (mailbox) ----------

    def sendNotification(
        self, destinationUUID, topic, messageObj, messageUUID, createdMs, tx=None
    ):
        """
        Insert one mailbox notification row.

        Args:
            destinationUUID (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            messageObj (dict): Message payload.
            messageUUID (str|None): Optional idempotency UUID.
            createdMs (int|long): Created timestamp.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of inserted rows.
        """
        sql = """
        INSERT INTO workflows.notifications(destination_uuid, topic, message_json, message_uuid, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?::uuid, ?)
        """
        n = self.update(
            sql,
            [
                destinationUUID,
                topic,
                _dumps(messageObj),
                messageUUID,
                long(createdMs),
            ],
            tx=tx,
        )
        if _isDebug():
            log.debug(
                "evt=db.mail.send dst=%s topic=%s mid=%s n=%s"
                % (destinationUUID, topic, messageUUID, n)
            )
        return n

    def recvNotification(self, destinationUUID, topic, timeoutMs, tx=None):
        """
        Non-blocking receive:
        - If timeoutMs == 0: fetch one oldest row and delete it atomically.
        - If timeoutMs > 0: loops with sleep (not good).
        NOTE IT WILL BLOCK if timeout > 0

        Args:
            destinationUUID (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            timeoutMs (int|long): Poll timeout in milliseconds.
            tx (str|None): Optional transaction id.

        Returns:
            dict|None: Decoded message payload, or `None` on timeout/no message.
        """

        deadline = nowMs() + long(timeoutMs)
        while True:
            row = self._popNotificationOnce(destinationUUID, topic, tx=tx)
            if row is not None:
                return row
            if timeoutMs <= 0 or nowMs() >= deadline:
                return None
            # TODO, is there a better way? this is it for now
            time.sleep(0.1)

    def _popNotificationOnce(self, destinationUUID, topic, tx=None):
        """
        Pop one mailbox message atomically using row lock + delete.
        Acknowledges the notification
        TODO this is what DBOS does, do we need this? we can keep all
         notifications as an audit instead

        Args:
            destinationUUID (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            tx (str|None): Optional transaction id.

        Returns:
            dict|None: Decoded message payload, or `None` when empty/error.
        """
        local_tx = None
        try:
            if tx is None:
                local_tx = self.begin()
                tx_use = local_tx
            else:
                tx_use = tx

            sel = """
            SELECT notification_id, message_json
            FROM workflows.notifications
            WHERE destination_uuid = ?::uuid AND topic = ?
            ORDER BY created_at_epoch_ms ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
            ds = self.query(sel, [destinationUUID, topic], tx=tx_use)
            if ds is None or ds.getRowCount() == 0:
                if local_tx is not None:
                    self.commit(local_tx)
                return None

            nid = ds.getValueAt(0, 0)
            msg = ds.getValueAt(0, 1)

            dele = "DELETE FROM workflows.notifications WHERE notification_id = ?"
            self.update(dele, [nid], tx=tx_use)

            if local_tx is not None:
                self.commit(local_tx)

            return _loads(msg)
        except Exception:
            if local_tx is not None:
                try:
                    self.rollback(local_tx)
                except:
                    self._logException(
                        "db.mail.pop.rollback.error",
                        "dst=%s topic=%s" % (destinationUUID, topic),
                    )
            self._logException(
                "db.mail.pop.error", "dst=%s topic=%s" % (destinationUUID, topic)
            )
            return None
        finally:
            if local_tx is not None:
                self.close(local_tx)

    # ---------- events ----------

    def upsertEvent(self, workflowId, key, valueObj, ms, tx=None):
        """
        Upsert current event value and append event history row.

        Args:
            workflowId (str): Workflow UUID.
            key (str): Event key.
            valueObj (object): Event payload.
            ms (int|long): Event timestamp.
            tx (str|None): Optional transaction id.

        Returns:
            None: Persists event rows in DB.
        """
        sql = """
        INSERT INTO workflows.workflow_events(workflow_id, key, value_json, updated_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?)
        ON CONFLICT (workflow_id, key)
        DO UPDATE SET value_json = EXCLUDED.value_json, updated_at_epoch_ms = EXCLUDED.updated_at_epoch_ms
        """
        self.update(sql, [workflowId, key, _dumps(valueObj), long(ms)], tx=tx)

        hist = """
        INSERT INTO workflows.workflow_events_history(workflow_id, key, value_json, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?)
        """
        self.update(hist, [workflowId, key, _dumps(valueObj), long(ms)], tx=tx)

        if _isDebug():
            log.debug(
                "evt=db.event.set wid=%s key=%s ms=%s val=%s"
                % (workflowId, key, long(ms), _truncate(valueObj, 300))
            )

    # ---------- streams ----------

    def appendStream(self, workflowId, key, valueObj, ms, tx=None):
        """
        Append to stream with atomic offset allocation via stream_heads.

        Args:
            workflowId (str): Workflow UUID.
            key (str): Stream key.
            value_obj (object): Stream payload.
            ms (int|long): Event timestamp.
            tx (str|None): Optional transaction id.

        Returns:
            long: Appended stream offset.
        """
        init = """
        INSERT INTO workflows.stream_heads(workflow_id, key, next_offset)
        VALUES (?::uuid, ?, 0)
        ON CONFLICT (workflow_id, key) DO NOTHING
        """
        self.update(init, [workflowId, key], tx=tx)

        alloc = """
        UPDATE workflows.stream_heads
        SET next_offset = next_offset + 1
        WHERE workflow_id = ?::uuid AND key = ?
        RETURNING next_offset
        """
        ds = self.query(alloc, [workflowId, key], tx=tx)
        next_off = long(ds.getValueAt(0, 0))
        off = next_off - 1

        ins = """
        INSERT INTO workflows.streams(workflow_id, key, "offset", value_json, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?, ?)
        """
        payload_json = _dumps(valueObj)
        self.update(ins, [workflowId, key, long(off), payload_json, long(ms)], tx=tx)

        if _isDebug():
            log.debug(
                "evt=db.stream.append wid=%s key=%s off=%s ms=%s val=%s"
                % (workflowId, key, long(off), long(ms), _truncate(valueObj, 300))
            )
        return off

    # ---------- retention ----------

    def _datasetFirstColAsStrList(self, ds):
        out = []
        if ds is None:
            return out
        for row_idx in range(ds.getRowCount()):
            out.append(str(ds.getValueAt(row_idx, 0)))
        return out

    def _deleteWorkflowTree(self, workflow_id, tx=None):
        deleted = 0
        for delete_sql in _DELETE_WORKFLOW_CHILD_SQL:
            deleted += self.update(delete_sql, [workflow_id], tx=tx)
        return deleted

    def _deleteWorkflowTrees(self, workflow_ids, tx=None):
        deleted = 0
        for workflow_id in workflow_ids:
            deleted += self._deleteWorkflowTree(workflow_id, tx=tx)
        return deleted

    def getRetentionConfig(self, tx=None):
        """
        Read retention configuration from `workflows.retention_config`.

        Args:
            tx (str|None): Optional transaction id.

        Returns:
            dict: Retention settings with fallback defaults when row is missing.
        """
        sql = "SELECT time_threshold_hours, rows_threshold, global_timeout_hours FROM workflows.retention_config WHERE id = 1"
        ds = self.query(sql, [], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            return {
                "time_threshold_hours": None,
                "rows_threshold": 1000000,
                "global_timeout_hours": None,
            }
        return {
            "time_threshold_hours": ds.getValueAt(0, 0),
            "rows_threshold": long(ds.getValueAt(0, 1)),
            "global_timeout_hours": ds.getValueAt(0, 2),
        }

    def enforceGlobalTimeout(self, hours, tx=None):
        """
        Cancel active workflows older than `hours`.

        Args:
            hours (float|int|None): Age threshold in hours.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows updated.
        """
        if hours is None or hours == 0:
            return 0
        cutoff = nowMs() - long(float(hours) * 3600.0 * 1000.0)
        err = _dumps({"type": "timeout", "reason": "global_timeout_hours exceeded"})
        sql = """
        UPDATE workflows.workflow_status
        SET status = ?,
            error_json = COALESCE(error_json, ?),
            updated_at_epoch_ms = ?,
            completed_at_epoch_ms = COALESCE(completed_at_epoch_ms, ?)
        WHERE status IN (?, ?, ?)
          AND created_at_epoch_ms < ?
        """
        ms = nowMs()
        n = self.update(
            sql,
            [
                settings.STATUS_CANCELLED,
                err,
                ms,
                ms,
                settings.STATUS_ENQUEUED,
                settings.STATUS_PENDING,
                settings.STATUS_RUNNING,
                cutoff,
            ],
            tx=tx,
        )
        log.info(
            "evt=db.retention.enforceGlobalTimeout hours=%s cutoff=%s n=%s"
            % (hours, cutoff, n)
        )
        return n

    def deleteOldTerminal(self, hours, tx=None):
        """
        Delete terminal workflows older than configured age, including child rows.

        Args:
            hours (float|int|None): Age threshold in hours.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows deleted across all affected tables.
        """
        if hours is None or hours == 0:
            return 0
        cutoff = nowMs() - long(float(hours) * 3600.0 * 1000.0)

        sel = """
        SELECT workflow_id
        FROM workflows.workflow_status
        WHERE status IN (?, ?, ?)
          AND COALESCE(completed_at_epoch_ms, updated_at_epoch_ms) < ?
        """
        ds = self.query(
            sel,
            list(_WORKFLOW_TERMINAL_FOR_DELETE) + [cutoff],
            tx=tx,
        )
        ids = self._datasetFirstColAsStrList(ds)
        if not ids:
            return 0

        deleted = self._deleteWorkflowTrees(ids, tx=tx)

        log.info(
            "evt=db.retention.deleteOldTerminal hours=%s cutoff=%s workflows=%s deleted_rows=%s"
            % (hours, cutoff, len(ids), deleted)
        )
        return deleted

    def enforceRowsThreshold(self, rowsThreshold, tx=None):
        """
        If workflow_status row count exceeds threshold, delete oldest terminal workflows.

        Args:
            rowsThreshold (int|long|None): Maximum allowed workflow_status rows.
            tx (str|None): Optional transaction id.

        Returns:
            int: Number of rows deleted across all affected tables.
        """
        if rowsThreshold is None or rowsThreshold == 0:
            return 0
        total = self.scalar(
            "SELECT COUNT(*) FROM workflows.workflow_status", [], tx=tx, default=0
        )
        total = long(total)
        if total <= long(rowsThreshold):
            return 0

        to_remove = total - long(rowsThreshold)
        if to_remove <= 0:
            return 0

        sel = """
        SELECT workflow_id
        FROM workflows.workflow_status
        WHERE status IN (?, ?, ?)
        ORDER BY COALESCE(completed_at_epoch_ms, updated_at_epoch_ms) ASC
        LIMIT ?
        """
        ds = self.query(
            sel,
            list(_WORKFLOW_TERMINAL_FOR_DELETE) + [long(to_remove)],
            tx=tx,
        )
        ids = self._datasetFirstColAsStrList(ds)
        if not ids:
            return 0

        deleted = self._deleteWorkflowTrees(ids, tx=tx)

        log.info(
            "evt=db.retention.enforceRowsThreshold threshold=%s total=%s removed=%s deleted_rows=%s"
            % (rowsThreshold, total, len(ids), deleted)
        )
        return deleted

    # ---------- row utils ----------

    def _rowToDict(self, ds, row_idx):
        col_names = ds.getColumnNames()
        out = {}
        for c in range(len(col_names)):
            k = col_names[c]
            v = ds.getValueAt(row_idx, c)
            out[k] = v
        return out
