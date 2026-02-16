# exchange/workflows/engine/db.py
"""
Database adapter and SQL helpers for workflows runtime.

Use this module for engine-internal SQL access only. Perspective/UI code should
continue using named queries where appropriate.

Logging goals:
- Make DB behavior observable in Ignition Gateway logs
- Record execution time + rowcounts
- Capture enough context to debug failures (SQL + args preview), without dumping huge payloads
- Keep compatibility with Jython 2.7

Timestamp semantics:
- created_at_epoch_ms: enqueue time
- claimed_at_epoch_ms: claim ownership/lease time
- started_at_epoch_ms: actual user-code start time
- deadline_epoch_ms: derived from started_at_epoch_ms + timeout (not claim time)
"""

import json
import traceback
from java.util import UUID
from java.lang import System as JSystem

from exchange.workflows import settings

log = system.util.getLogger(settings.getLoggerName("engine.db"))

_WORKFLOW_TERMINAL = tuple(settings.WORKFLOW_TERMINAL_STATUSES)

# ----------------------------
# Logging controls (runtime configurable)
# ----------------------------

_DEBUG_FLAG_KEY = "exchange.workflows.db.debug"  # bool
_SLOW_MS_KEY = "exchange.workflows.db.slow_ms"  # int
_LOG_ARGS_KEY = "exchange.workflows.db.log_args"  # bool

_DEFAULT_SLOW_MS = 250
_DEFAULT_LOG_ARGS = True


def now_ms():
    """
    Return current epoch time in milliseconds.

    Args:
        None.

    Returns:
        long: Current epoch milliseconds.
    """
    return system.date.toMillis(system.date.now())


def uuid4():
    """
    Generate a UUID string for workflow and message identifiers.

    Args:
        None.

    Returns:
        str: Random UUID string.
    """
    return str(UUID.randomUUID())


def _dumps(obj):
    """
    Serialize an object to compact JSON for DB persistence.

    Args:
        obj (object): Value to serialize.

    Returns:
        str|None: JSON string, or ``None`` when input is ``None``.
    """
    if obj is None:
        return None
    return json.dumps(obj, separators=(",", ":"))


def _loads(s):
    """
    Deserialize JSON text from DB columns into Python objects.

    Args:
        s (str|dict|list|None): Stored JSON text or already-decoded object.

    Returns:
        object: Decoded object, passthrough dict/list, or ``None`` on parse error.
    """
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except:
        return None


def _get_store():
    """
    Return global settings store used for DB debug flags.

    Args:
        None.

    Returns:
        tuple: ``(store, storeType)`` where store is mutable when available.
    """
    try:
        if hasattr(system.util, "globalVarMap"):
            return system.util.globalVarMap(_DEBUG_FLAG_KEY), "globalVarMap"
    except:
        pass
    try:
        return system.util.getGlobals(), "globals"
    except:
        return None, "none"


def _cfg_bool(key, default=False):
    """
    Read a boolean config value from global store.

    Args:
        key (str): Global config key.
        default (bool): Fallback when key is missing or invalid.

    Returns:
        bool: Parsed boolean value.
    """
    store, _t = _get_store()
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


def _cfg_int(key, default=0):
    """
    Read an integer config value from global store.

    Args:
        key (str): Global config key.
        default (int): Fallback when key is missing or invalid.

    Returns:
        int: Parsed integer value.
    """
    store, _t = _get_store()
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


def _is_debug():
    """
    Return whether verbose DB debug logging is enabled.

    Args:
        None.

    Returns:
        bool: Debug logging flag.
    """
    return False  # _cfg_bool(_DEBUG_FLAG_KEY, default=False)


def _slow_ms():
    """
    Return slow-query threshold used for warning logs.

    Args:
        None.

    Returns:
        int: Slow query threshold in milliseconds.
    """
    return 2000  # _cfg_int(_SLOW_MS_KEY, default=_DEFAULT_SLOW_MS)


def _log_args_enabled():
    """
    Return whether SQL argument previews should be logged.

    Args:
        None.

    Returns:
        bool: Argument logging flag.
    """
    return True  # _cfg_bool(_LOG_ARGS_KEY, default=_DEFAULT_LOG_ARGS)


def _truncate(s, max_len=300):
    """
    Truncate large values for safer logs.

    Args:
        s (object): Value to render.
        max_len (int): Maximum output string length.

    Returns:
        str|None: Truncated string representation.
    """
    try:
        if s is None:
            return None
        s2 = str(s)
        if len(s2) <= int(max_len):
            return s2
        return s2[: int(max_len)] + "...(trunc)"
    except:
        return "<unprintable>"


def _safe_arg(v):
    """
    Prevent logs from exploding:
    - truncate long strings/json
    - summarize dict/list
    """
    try:
        if v is None:
            return None
        if isinstance(v, (int, long, float, bool)):
            return v
        # Ignition often hands java objects; stringify carefully
        if isinstance(v, (dict, list)):
            try:
                # compact summary
                return _truncate(json.dumps(v, separators=(",", ":")), 300)
            except:
                return _truncate(v, 300)
        return _truncate(v, 300)
    except:
        return "<arg:unprintable>"


def _args_preview(args):
    """
    Build a compact argument preview string for query/update logs.

    Args:
        args (list|tuple|None): Prepared-statement argument list.

    Returns:
        str: Human-readable argument preview.
    """
    if not _log_args_enabled():
        return "args=<suppressed>"
    try:
        if args is None:
            return "args=[]"
        out = []
        for a in list(args):
            out.append(_safe_arg(a))
        return "args=%s" % _truncate(out, 500)
    except:
        return "args=<error>"


def _sql_preview(sql):
    """
    Normalize SQL text for compact single-line logging.

    Args:
        sql (str): SQL statement text.

    Returns:
        str: Normalized SQL preview string.
    """
    # Normalize whitespace for easier grep
    try:
        s = " ".join(str(sql).split())
        return _truncate(s, 600)
    except:
        return "<sql:unprintable>"


def _tx_id(tx):
    """
    Build a safe transaction identifier for logs.

    Args:
        tx (object): Ignition transaction handle/id.

    Returns:
        str: String transaction id or placeholder.
    """
    try:
        if tx is None:
            return "-"
        # tx is a string in Ignition (transaction id)
        return str(tx)
    except:
        return "<tx>"


def _elapsed_ms(t0_ms):
    """
    Compute elapsed milliseconds since a start timestamp.

    Args:
        t0_ms (int|long): Start timestamp in epoch milliseconds.

    Returns:
        long: Elapsed milliseconds, or ``-1`` on conversion error.
    """
    try:
        return long(now_ms() - long(t0_ms))
    except:
        return -1


# ----------------------------
# DB adapter
# ----------------------------


class DB(object):
    """
    Lightweight DB adapter around system.db.* for Postgres.

    Args:
        db_name (str): Ignition DB connection name.
    """

    def __init__(self, db_name):
        """
        Build a DB adapter bound to one Ignition database connection.

        Args:
            db_name (str): Ignition DB connection name.

        Returns:
            None: Stores DB connection name on `self`.
        """
        self.db_name = db_name

    # ---------- transaction helpers ----------

    def begin(self):
        """
        Start a database transaction.

        Args:
            None.

        Returns:
            object: Ignition transaction handle/id.
        """
        t0 = now_ms()
        try:
            tx = system.db.beginTransaction(self.db_name)
            dt = _elapsed_ms(t0)
            #            log.info("evt=db.tx.begin db=%s tx=%s ms=%d" % (self.db_name, _tx_id(tx), dt))
            return tx
        except Exception as e:
            dt = _elapsed_ms(t0)
            log.error(
                "evt=db.tx.begin.error db=%s ms=%d err=%s" % (self.db_name, dt, e)
            )
            log.error("evt=db.tx.begin.trace trace=%s" % traceback.format_exc())
            raise

    def commit(self, tx):
        """
        Commit a database transaction.

        Args:
            tx (object): Ignition transaction handle/id.

        Returns:
            None: Commits the transaction in place.
        """
        t0 = now_ms()
        try:
            system.db.commitTransaction(tx)
            dt = _elapsed_ms(t0)
        #            log.info("evt=db.tx.commit db=%s tx=%s ms=%d" % (self.db_name, _tx_id(tx), dt))
        except Exception as e:
            dt = _elapsed_ms(t0)
            log.error(
                "evt=db.tx.commit.error db=%s tx=%s ms=%d err=%s"
                % (self.db_name, _tx_id(tx), dt, e)
            )
            log.error("evt=db.tx.commit.trace trace=%s" % traceback.format_exc())
            raise

    def rollback(self, tx):
        """
        Roll back a database transaction.

        Args:
            tx (object): Ignition transaction handle/id.

        Returns:
            None: Rolls back the transaction in place.
        """
        t0 = now_ms()
        try:
            system.db.rollbackTransaction(tx)
            dt = _elapsed_ms(t0)
            log.warn(
                "evt=db.tx.rollback db=%s tx=%s ms=%d" % (self.db_name, _tx_id(tx), dt)
            )
        except Exception as e:
            dt = _elapsed_ms(t0)
            log.error(
                "evt=db.tx.rollback.error db=%s tx=%s ms=%d err=%s"
                % (self.db_name, _tx_id(tx), dt, e)
            )
            log.error("evt=db.tx.rollback.trace trace=%s" % traceback.format_exc())
            # don't raise rollback errors by default

    def close(self, tx):
        """
        Close a database transaction handle.

        Args:
            tx (object): Ignition transaction handle/id.

        Returns:
            None: Closes transaction resources.
        """
        t0 = now_ms()
        try:
            system.db.closeTransaction(tx)
            dt = _elapsed_ms(t0)
            if _is_debug():
                log.debug(
                    "evt=db.tx.close db=%s tx=%s ms=%d" % (self.db_name, _tx_id(tx), dt)
                )
        except:
            # ignore
            pass

    # ---------- core query/update wrappers with logging ----------

    def q(self, sql, args=None, tx=None):
        """
        Execute a prepared query with timing and context logging.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (object|None): Optional transaction handle.

        Returns:
            PyDataSet: Query result dataset.
        """
        args = args or []
        t0 = now_ms()
        try:
            if tx is None:
                ds = system.db.runPrepQuery(sql, args, self.db_name)
            else:
                ds = system.db.runPrepQuery(sql, args, self.db_name, tx)

            dt = _elapsed_ms(t0)
            rows = -1
            try:
                rows = ds.getRowCount() if ds is not None else 0
            except:
                rows = -1

            if dt >= _slow_ms():
                log.warn(
                    "evt=db.q.slow db=%s tx=%s ms=%d rows=%s sql=%s %s"
                    % (
                        self.db_name,
                        _tx_id(tx),
                        dt,
                        rows,
                        _sql_preview(sql),
                        _args_preview(args),
                    )
                )
            elif _is_debug():
                log.debug(
                    "evt=db.q db=%s tx=%s ms=%d rows=%s sql=%s %s"
                    % (
                        self.db_name,
                        _tx_id(tx),
                        dt,
                        rows,
                        _sql_preview(sql),
                        _args_preview(args),
                    )
                )
            return ds
        except Exception as e:
            dt = _elapsed_ms(t0)
            log.error(
                "evt=db.q.error db=%s tx=%s ms=%d err=%s sql=%s %s"
                % (
                    self.db_name,
                    _tx_id(tx),
                    dt,
                    e,
                    _sql_preview(sql),
                    _args_preview(args),
                )
            )
            log.error("evt=db.q.trace trace=%s" % traceback.format_exc())
            raise

    def u(self, sql, args=None, tx=None):
        """
        Execute a prepared update with timing and context logging.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of affected rows.
        """
        args = args or []
        t0 = now_ms()
        try:
            if tx is None:
                n = system.db.runPrepUpdate(sql, args, self.db_name)
            else:
                n = system.db.runPrepUpdate(sql, args, self.db_name, tx)

            dt = _elapsed_ms(t0)
            if dt >= _slow_ms():
                log.warn(
                    "evt=db.u.slow db=%s tx=%s ms=%d n=%s sql=%s %s"
                    % (
                        self.db_name,
                        _tx_id(tx),
                        dt,
                        n,
                        _sql_preview(sql),
                        _args_preview(args),
                    )
                )
            elif _is_debug():
                log.debug(
                    "evt=db.u db=%s tx=%s ms=%d n=%s sql=%s %s"
                    % (
                        self.db_name,
                        _tx_id(tx),
                        dt,
                        n,
                        _sql_preview(sql),
                        _args_preview(args),
                    )
                )
            return n
        except Exception as e:
            dt = _elapsed_ms(t0)
            log.error(
                "evt=db.u.error db=%s tx=%s ms=%d err=%s sql=%s %s"
                % (
                    self.db_name,
                    _tx_id(tx),
                    dt,
                    e,
                    _sql_preview(sql),
                    _args_preview(args),
                )
            )
            log.error("evt=db.u.trace trace=%s" % traceback.format_exc())
            raise

    def scalar(self, sql, args=None, tx=None, default=None):
        """
        Run a query and return the first column of the first row.

        Args:
            sql (str): SQL statement text.
            args (list|tuple|None): Prepared-statement parameters.
            tx (object|None): Optional transaction handle.
            default (object): Value returned when the result set is empty.

        Returns:
            object: Scalar query result or `default`.
        """
        t0 = now_ms()
        try:
            ds = self.q(sql, args=args, tx=tx)
            if ds is None or ds.getRowCount() == 0:
                if _is_debug():
                    log.debug(
                        "evt=db.scalar.miss db=%s tx=%s ms=%d sql=%s %s"
                        % (
                            self.db_name,
                            _tx_id(tx),
                            _elapsed_ms(t0),
                            _sql_preview(sql),
                            _args_preview(args),
                        )
                    )
                return default
            v = ds.getValueAt(0, 0)
            if _is_debug():
                log.debug(
                    "evt=db.scalar.hit db=%s tx=%s ms=%d val=%s sql=%s %s"
                    % (
                        self.db_name,
                        _tx_id(tx),
                        _elapsed_ms(t0),
                        _truncate(v, 200),
                        _sql_preview(sql),
                        _args_preview(args),
                    )
                )
            return v
        except:
            # q() already logs errors
            raise

    # ---------- workflow status ----------

    def insert_workflow(
        self,
        workflow_id,
        workflow_name,
        queue_name,
        partition_key,
        priority,
        deduplication_id,
        application_version,
        inputs_obj,
        created_ms,
        deadline_ms=None,
        tx=None,
    ):
        """
        Insert one ENQUEUED workflow row.

        Args:
            workflow_id (str): Workflow UUID.
            workflow_name (str): Registered workflow name.
            queue_name (str): Queue routing name.
            partition_key (str|None): Optional partition arbitration key.
            priority (int): Queue priority.
            deduplication_id (str|None): Optional dedupe key.
            application_version (str|None): Optional app/version stamp.
            inputs_obj (dict|None): Serialized workflow inputs.
            created_ms (int|long): Created timestamp in epoch ms.
            deadline_ms (int|long|None): Optional deadline timestamp.
            tx (object|None): Optional transaction handle.

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
        n = self.u(
            sql,
            [
                workflow_id,
                workflow_name,
                settings.STATUS_ENQUEUED,
                queue_name,
                partition_key,
                int(priority),
                deduplication_id,
                application_version,
                _dumps(inputs_obj),
                long(created_ms),
                long(created_ms),
                long(deadline_ms) if deadline_ms is not None else None,
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.workflow.insert wid=%s wf=%s n=%s"
                % (workflow_id, workflow_name, n)
            )
        return n

    def insert_workflows_batch(self, rows, tx=None):
        """
        Insert multiple workflow rows in a single transaction context.

        Args:
            rows (list[dict]): Row payloads matching `insert_workflow` arguments.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows inserted.
        """
        if not rows:
            return 0
        inserted = 0
        for row in rows:
            self.insert_workflow(
                workflow_id=row.get("workflow_id"),
                workflow_name=row.get("workflow_name"),
                queue_name=row.get("queue_name"),
                partition_key=row.get("partition_key"),
                priority=int(row.get("priority") or 0),
                deduplication_id=row.get("deduplication_id"),
                application_version=row.get("application_version"),
                inputs_obj=row.get("inputs_obj"),
                created_ms=long(row.get("created_ms") or now_ms()),
                deadline_ms=row.get("deadline_ms"),
                tx=tx,
            )
            inserted += 1
        return inserted

    def get_workflow(self, workflow_id, tx=None):
        """
        Fetch one workflow_status row by workflow id.

        Args:
            workflow_id (str): Workflow UUID.
            tx (object|None): Optional transaction handle.

        Returns:
            dict|None: Workflow row dict, or `None` when not found.
        """
        sql = "SELECT * FROM workflows.workflow_status WHERE workflow_id = ?::uuid"
        ds = self.q(sql, [workflow_id], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            if _is_debug():
                log.debug("evt=db.workflow.get.miss wid=%s" % workflow_id)
            return None
        row = self._row_to_dict(ds, 0)
        if _is_debug():
            log.debug(
                "evt=db.workflow.get.hit wid=%s status=%s"
                % (workflow_id, row.get("status"))
            )
        return row

    def update_status(self, workflow_id, status, fields=None, tx=None):
        """
        Update workflow status plus optional additional columns.

        Args:
            workflow_id (str): Workflow UUID.
            status (str): New workflow status value.
            fields (dict|None): Additional columns to update.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of updated rows.
        """
        fields = fields or {}
        sets = ["status = ?", "updated_at_epoch_ms = ?"]
        args = [status, now_ms()]
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.append(workflow_id)
        sql = "UPDATE workflows.workflow_status SET %s WHERE workflow_id = ?::uuid" % (
            ", ".join(sets)
        )
        n = self.u(sql, args, tx=tx)
        if _is_debug():
            log.debug(
                "evt=db.workflow.update_status wid=%s status=%s n=%s fields=%s"
                % (workflow_id, status, n, _truncate(fields, 400))
            )
        return n

    def update_status_if_not_terminal(self, workflow_id, status, fields=None, tx=None):
        """
        Update workflow status only when current status is not terminal.

        Args:
            workflow_id (str): Workflow UUID.
            status (str): New workflow status value.
            fields (dict|None): Additional columns to update.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of updated rows (0 when already terminal).
        """
        fields = fields or {}
        sets = ["status = ?", "updated_at_epoch_ms = ?"]
        args = [status, now_ms()]
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.append(workflow_id)
        args.extend(list(_WORKFLOW_TERMINAL))
        sql = (
            "UPDATE workflows.workflow_status SET %s "
            "WHERE workflow_id = ?::uuid "
            "AND status NOT IN (?, ?, ?)"
        ) % (", ".join(sets))
        n = self.u(sql, args, tx=tx)
        if _is_debug():
            log.debug(
                "evt=db.workflow.update_status_if_not_terminal wid=%s status=%s n=%s fields=%s"
                % (workflow_id, status, n, _truncate(fields, 400))
            )
        return n

    def mark_running_if_claimed(
        self,
        workflow_id,
        executor_id,
        started_ms=None,
        timeout_ms=None,
        fields=None,
        tx=None,
    ):
        """
        Transition PENDING workflow to RUNNING only when claimed by this executor.

        This is the point where execution officially starts. It sets:
        - started_at_epoch_ms = COALESCE(started_at_epoch_ms, started_ms)
        - deadline_epoch_ms = COALESCE(deadline_epoch_ms, started_ms + timeout_ms)

        Args:
            workflow_id (str): Workflow UUID.
            executor_id (str): Executor id expected in `claimed_by`.
            started_ms (int|long|None): Execution start timestamp; defaults to `now_ms()`.
            timeout_ms (int|long|None): Timeout duration in ms to derive deadline at start.
            fields (dict|None): Additional fields to update on success.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of updated rows.
        """
        fields = fields or {}
        if started_ms is None:
            started_ms = now_ms()
        else:
            started_ms = long(started_ms)
        sets = [
            "status = ?",
            "updated_at_epoch_ms = ?",
            "started_at_epoch_ms = COALESCE(started_at_epoch_ms, ?)",
            "heartbeat_at_epoch_ms = ?",
        ]
        args = [settings.STATUS_RUNNING, started_ms, started_ms, started_ms]
        if timeout_ms is not None:
            timeout_ms = long(timeout_ms)
            if timeout_ms > 0:
                sets.append("deadline_epoch_ms = COALESCE(deadline_epoch_ms, ?)")
                args.append(long(started_ms + timeout_ms))
        for k, v in fields.items():
            sets.append("%s = ?" % k)
            args.append(v)
        args.extend([workflow_id, settings.STATUS_PENDING, executor_id])
        sql = (
            "UPDATE workflows.workflow_status SET %s "
            "WHERE workflow_id = ?::uuid "
            "AND status = ? "
            "AND claimed_by = ?"
        ) % (", ".join(sets))
        n = self.u(sql, args, tx=tx)
        if _is_debug():
            log.debug(
                "evt=db.workflow.mark_running_if_claimed wid=%s exec=%s n=%s"
                % (workflow_id, executor_id, n)
            )
        return n

    def release_claim(self, workflow_id, tx=None):
        """
        Release a PENDING claim back to ENQUEUED.

        Args:
            workflow_id (str): Workflow UUID.
            tx (object|None): Optional transaction handle.

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
        ms = now_ms()
        n = self.u(
            sql,
            [settings.STATUS_ENQUEUED, ms, workflow_id, settings.STATUS_PENDING],
            tx=tx,
        )
        if _is_debug():
            log.debug("evt=db.workflow.release_claim wid=%s n=%s" % (workflow_id, n))
        return n

    def cancel_expired(self, tx=None):
        """
        Cancel workflows whose deadline has expired.

        Args:
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of cancelled rows.
        """
        return self._cancel_where(
            "deadline_epoch_ms IS NOT NULL AND deadline_epoch_ms < ?",
            [now_ms()],
            "Timeout exceeded",
            cancel_type="timeout",
            tx=tx,
        )

    def cancel_workflow(self, workflow_id, reason, tx=None):
        """
        Cooperatively cancel one workflow (workflow code should poll/check).

        Args:
            workflow_id (str): Workflow UUID.
            reason (str): Human-readable cancellation reason.
            tx (object|None): Optional transaction handle.

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
        ms = now_ms()
        n = self.u(
            sql,
            [
                settings.STATUS_CANCELLED,
                err,
                ms,
                ms,
                workflow_id,
                settings.STATUS_ENQUEUED,
                settings.STATUS_PENDING,
                settings.STATUS_RUNNING,
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.workflow.cancel wid=%s n=%s reason=%s"
                % (workflow_id, n, _truncate(reason, 200))
            )
        return n

    def _cancel_where(
        self, where_sql, where_args, reason, cancel_type="cancelled", tx=None
    ):
        """
        Cancel workflows matching a WHERE clause.

        Args:
            where_sql (str): SQL predicate fragment without `WHERE`.
            where_args (list): Prepared-statement values for the predicate.
            reason (str): Human-readable cancellation reason.
            cancel_type (str): Error payload cancellation type.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows updated.
        """
        err = _dumps({"type": cancel_type, "reason": reason})
        ms = now_ms()
        sql = (
            """
        UPDATE workflows.workflow_status
        SET status = ?,
            error_json = COALESCE(error_json, ?),
            updated_at_epoch_ms = ?,
            completed_at_epoch_ms = COALESCE(completed_at_epoch_ms, ?)
        WHERE %s
          AND status IN (?, ?, ?)
        """
            % where_sql
        )
        args = [settings.STATUS_CANCELLED, err, ms, ms] + list(where_args) + [
            settings.STATUS_ENQUEUED,
            settings.STATUS_PENDING,
            settings.STATUS_RUNNING,
        ]
        n = self.u(sql, args, tx=tx)
        if _is_debug():
            log.debug(
                "evt=db.workflow.cancel_where n=%s type=%s reason=%s where=%s"
                % (n, cancel_type, _truncate(reason, 200), _truncate(where_sql, 200))
            )
        return n

    def cancel_enqueued_pending(self, queue_name=None, reason="", tx=None):
        """
        Cancel ENQUEUED and PENDING workflows, optionally scoped to one queue.

        Args:
            queue_name (str|None): Queue filter.
            reason (str): Human-readable cancellation reason.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows updated.
        """
        where_sql = "status IN (?, ?)"
        where_args = [settings.STATUS_ENQUEUED, settings.STATUS_PENDING]
        if queue_name is not None:
            where_sql += " AND queue_name = ?"
            where_args.append(queue_name)
        return self._cancel_where(
            where_sql,
            where_args,
            reason or "maintenance_cancel",
            cancel_type="maintenance_cancel",
            tx=tx,
        )

    def cancel_running(self, queue_name=None, reason="", tx=None):
        """
        Cancel RUNNING workflows, optionally scoped to one queue.

        Args:
            queue_name (str|None): Queue filter.
            reason (str): Human-readable cancellation reason.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows updated.
        """
        where_sql = "status = ?"
        where_args = [settings.STATUS_RUNNING]
        if queue_name is not None:
            where_sql += " AND queue_name = ?"
            where_args.append(queue_name)
        return self._cancel_where(
            where_sql,
            where_args,
            reason or "maintenance_cancel",
            cancel_type="maintenance_cancel",
            tx=tx,
        )

    # ---------- claiming (queue tick) ----------

    def claim_enqueued(self, queue_name, max_to_claim, executor_id, tx):
        """
        Claim up to N ENQUEUED items for the given queue, using row locks.

        Partition arbitration is DB-level:
        - if partition_key is not null, exclude those partitions that already have PENDING/RUNNING.
        - claim transition only sets PENDING + claim metadata (not started_at/deadline)

        Args:
            queue_name (str): Queue to claim from.
            max_to_claim (int): Max number of rows to claim.
            executor_id (str): Executor identifier persisted in claim metadata.
            tx (object): Transaction handle (required).

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
        ORDER BY ws.priority DESC, ws.created_at_epoch_ms ASC
        FOR UPDATE SKIP LOCKED
        LIMIT ?
        """
        ids_ds = self.q(
            sql,
            [
                queue_name,
                settings.STATUS_ENQUEUED,
                settings.STATUS_PENDING,
                settings.STATUS_RUNNING,
                int(max_to_claim),
            ],
            tx=tx,
        )
        ids = []
        for r in range(ids_ds.getRowCount()):
            ids.append(str(ids_ds.getValueAt(r, 0)))

        if _is_debug():
            log.debug(
                "evt=db.claim.ids queue=%s want=%s got=%s exec=%s"
                % (queue_name, int(max_to_claim), len(ids), executor_id)
            )

        claimed = []
        ms = now_ms()
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
            n = self.u(
                upd,
                [
                    settings.STATUS_PENDING,
                    executor_id,
                    ms,
                    ms,
                    ms,
                    wid,
                    settings.STATUS_ENQUEUED,
                ],
                tx=tx,
            )
            if n == 1:
                row = self.get_workflow(wid, tx=tx)
                if row:
                    claimed.append(row)

        if _is_debug():
            log.debug(
                "evt=db.claim.done queue=%s claimed=%s" % (queue_name, len(claimed))
            )
        return claimed

    # ---------- step outputs (durable replay) ----------

    def get_step_output(self, workflow_id, call_seq, tx=None):
        """
        Read cached step output/error row for deterministic replay.

        Args:
            workflow_id (str): Workflow UUID.
            call_seq (int): Deterministic step call sequence.
            tx (object|None): Optional transaction handle.

        Returns:
            dict|None: Step cache record, or `None` when not found.
        """
        sql = """
        SELECT status, output_json, error_json, attempts
        FROM workflows.operation_outputs
        WHERE workflow_id = ?::uuid AND call_seq = ?
        """
        ds = self.q(sql, [workflow_id, int(call_seq)], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            return None
        return {
            "status": ds.getValueAt(0, 0),
            "output": _loads(ds.getValueAt(0, 1)),
            "error": _loads(ds.getValueAt(0, 2)),
            "attempts": int(ds.getValueAt(0, 3)),
        }

    def insert_step_attempt(
        self, workflow_id, call_seq, step_name, started_ms, tx=None
    ):
        """
        Insert initial step attempt row when it does not already exist.

        Args:
            workflow_id (str): Workflow UUID.
            call_seq (int): Deterministic step call sequence.
            step_name (str): Step registry name.
            started_ms (int|long): Step start timestamp.
            tx (object|None): Optional transaction handle.

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
        n = self.u(
            sql,
            [
                workflow_id,
                int(call_seq),
                step_name,
                settings.STEP_STATUS_STARTED,
                long(started_ms),
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.step.insert_attempt wid=%s call_seq=%s step=%s n=%s"
                % (workflow_id, int(call_seq), step_name, n)
            )
        return n

    def update_step_success(
        self, workflow_id, call_seq, output_obj, completed_ms, attempts, tx=None
    ):
        """
        Mark a step attempt as SUCCESS and persist output JSON.

        Args:
            workflow_id (str): Workflow UUID.
            call_seq (int): Deterministic step call sequence.
            output_obj (object): Step return payload.
            completed_ms (int|long): Completion timestamp.
            attempts (int): Attempt count used for this success.
            tx (object|None): Optional transaction handle.

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
        n = self.u(
            sql,
            [
                settings.STATUS_SUCCESS,
                _dumps(output_obj),
                long(completed_ms),
                int(attempts),
                workflow_id,
                int(call_seq),
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.step.success wid=%s call_seq=%s attempts=%s n=%s"
                % (workflow_id, int(call_seq), int(attempts), n)
            )
        return n

    def update_step_error(
        self, workflow_id, call_seq, error_obj, completed_ms, attempts, tx=None
    ):
        """
        Mark a step attempt as ERROR and persist error JSON.

        Args:
            workflow_id (str): Workflow UUID.
            call_seq (int): Deterministic step call sequence.
            error_obj (dict): Error payload (message/traceback/attempt info).
            completed_ms (int|long): Completion timestamp.
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
        n = self.u(
            sql,
            [
                settings.STATUS_ERROR,
                _dumps(error_obj),
                long(completed_ms),
                int(attempts),
                workflow_id,
                int(call_seq),
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.step.error wid=%s call_seq=%s attempts=%s n=%s err=%s"
                % (
                    workflow_id,
                    int(call_seq),
                    int(attempts),
                    n,
                    _truncate(error_obj, 300),
                )
            )
        return n

    # ---------- notifications (mailbox) ----------

    def send_notification(
        self, destination_uuid, topic, message_obj, message_uuid, created_ms, tx=None
    ):
        """
        Insert one mailbox notification row.

        Args:
            destination_uuid (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            message_obj (dict): Message payload.
            message_uuid (str|None): Optional idempotency UUID.
            created_ms (int|long): Created timestamp.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of inserted rows.
        """
        sql = """
        INSERT INTO workflows.notifications(destination_uuid, topic, message_json, message_uuid, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?::uuid, ?)
        """
        n = self.u(
            sql,
            [
                destination_uuid,
                topic,
                _dumps(message_obj),
                message_uuid,
                long(created_ms),
            ],
            tx=tx,
        )
        if _is_debug():
            log.debug(
                "evt=db.mail.send dst=%s topic=%s mid=%s n=%s"
                % (destination_uuid, topic, message_uuid, n)
            )
        return n

    def recv_notification(self, destination_uuid, topic, timeout_ms, tx=None):
        """
        Non-blocking receive:
        - If timeout_ms == 0: fetch one oldest row and delete it atomically.
        - If timeout_ms > 0: loops with sleep (kept simple).

        Args:
            destination_uuid (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            timeout_ms (int|long): Poll timeout in milliseconds.
            tx (object|None): Optional transaction handle.

        Returns:
            dict|None: Decoded message payload, or `None` on timeout/no message.
        """
        import time as _time

        deadline = now_ms() + long(timeout_ms)
        while True:
            row = self._pop_notification_once(destination_uuid, topic, tx=tx)
            if row is not None:
                return row
            if timeout_ms <= 0 or now_ms() >= deadline:
                return None
            _time.sleep(0.1)

    def _pop_notification_once(self, destination_uuid, topic, tx=None):
        """
        Pop one mailbox message atomically using row lock + delete.

        Args:
            destination_uuid (str): Destination workflow UUID.
            topic (str): Mailbox topic.
            tx (object|None): Optional transaction handle.

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
            ds = self.q(sel, [destination_uuid, topic], tx=tx_use)
            if ds is None or ds.getRowCount() == 0:
                if local_tx is not None:
                    self.commit(local_tx)
                return None

            nid = ds.getValueAt(0, 0)
            msg = ds.getValueAt(0, 1)

            dele = "DELETE FROM workflows.notifications WHERE notification_id = ?"
            self.u(dele, [nid], tx=tx_use)

            if local_tx is not None:
                self.commit(local_tx)

            return _loads(msg)
        except Exception:
            if local_tx is not None:
                try:
                    self.rollback(local_tx)
                except:
                    pass
            if _is_debug():
                log.error(
                    "evt=db.mail.pop.error dst=%s topic=%s trace=%s"
                    % (destination_uuid, topic, traceback.format_exc())
                )
            return None
        finally:
            if local_tx is not None:
                self.close(local_tx)

    # ---------- events ----------

    def set_event(self, workflow_id, key, value_obj, ms, tx=None):
        """
        Upsert current event value and append event history row.

        Args:
            workflow_id (str): Workflow UUID.
            key (str): Event key.
            value_obj (object): Event payload.
            ms (int|long): Event timestamp.
            tx (object|None): Optional transaction handle.

        Returns:
            None: Persists event rows in DB.
        """
        sql = """
        INSERT INTO workflows.workflow_events(workflow_id, key, value_json, updated_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?)
        ON CONFLICT (workflow_id, key)
        DO UPDATE SET value_json = EXCLUDED.value_json, updated_at_epoch_ms = EXCLUDED.updated_at_epoch_ms
        """
        self.u(sql, [workflow_id, key, _dumps(value_obj), long(ms)], tx=tx)

        hist = """
        INSERT INTO workflows.workflow_events_history(workflow_id, key, value_json, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?)
        """
        self.u(hist, [workflow_id, key, _dumps(value_obj), long(ms)], tx=tx)

        if _is_debug():
            log.debug(
                "evt=db.event.set wid=%s key=%s ms=%s val=%s"
                % (workflow_id, key, long(ms), _truncate(value_obj, 300))
            )

    # ---------- streams ----------

    def append_stream(self, workflow_id, key, value_obj, ms, tx=None):
        """
        Append to stream with atomic offset allocation via stream_heads.

        Args:
            workflow_id (str): Workflow UUID.
            key (str): Stream key.
            value_obj (object): Stream payload.
            ms (int|long): Event timestamp.
            tx (object|None): Optional transaction handle.

        Returns:
            long: Appended stream offset.
        """
        init = """
        INSERT INTO workflows.stream_heads(workflow_id, key, next_offset)
        VALUES (?::uuid, ?, 0)
        ON CONFLICT (workflow_id, key) DO NOTHING
        """
        self.u(init, [workflow_id, key], tx=tx)

        alloc = """
        UPDATE workflows.stream_heads
        SET next_offset = next_offset + 1
        WHERE workflow_id = ?::uuid AND key = ?
        RETURNING next_offset
        """
        ds = self.q(alloc, [workflow_id, key], tx=tx)
        next_off = long(ds.getValueAt(0, 0))
        off = next_off - 1

        ins = """
        INSERT INTO workflows.streams(workflow_id, key, "offset", value_json, created_at_epoch_ms)
        VALUES (?::uuid, ?, ?, ?, ?)
        """
        if True or _is_debug():
            log.debug(
                "evt=db.stream.append wid=%s TRYING TO APPEND this =%s"
                % (workflow_id, ins)
            )
        if True or _is_debug():
            log.debug(
                "evt=db.stream.append wid=%s TRYING TO APPEND values =%s"
                % (
                    workflow_id,
                    [workflow_id, key, long(off), _dumps(value_obj), long(ms)],
                )
            )
        self.u(ins, [workflow_id, key, long(off), _dumps(value_obj), long(ms)], tx=tx)

        if True or _is_debug():
            log.debug(
                "evt=db.stream.append wid=%s key=%s off=%s ms=%s val=%s"
                % (workflow_id, key, long(off), long(ms), _truncate(value_obj, 300))
            )
        return off

    # ---------- parameter templates/sets ----------

    def get_latest_template(self, workflow_name, template_name, tx=None):
        """
        Fetch the latest active parameter template for a workflow/template name.

        Args:
            workflow_name (str): Workflow name.
            template_name (str): Template name.
            tx (object|None): Optional transaction handle.

        Returns:
            dict|None: Template version + schema payload, or `None`.
        """
        sql = """
        SELECT template_version, schema_json
        FROM workflows.param_templates
        WHERE workflow_name = ? AND template_name = ? AND status = 'active'
        ORDER BY template_version DESC
        LIMIT 1
        """
        ds = self.q(sql, [workflow_name, template_name], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            return None
        return {
            "template_version": int(ds.getValueAt(0, 0)),
            "schema": _loads(ds.getValueAt(0, 1)),
        }

    def get_latest_set(self, workflow_name, template_name, set_name, tx=None):
        """
        Fetch the latest active parameter set for a workflow/template/set key.

        Args:
            workflow_name (str): Workflow name.
            template_name (str): Template name.
            set_name (str): Set name.
            tx (object|None): Optional transaction handle.

        Returns:
            dict|None: Set version + values payload, or `None`.
        """
        sql = """
        SELECT set_version, values_json
        FROM workflows.param_sets
        WHERE workflow_name = ? AND template_name = ? AND set_name = ? AND status = 'active'
        ORDER BY set_version DESC
        LIMIT 1
        """
        ds = self.q(sql, [workflow_name, template_name, set_name], tx=tx)
        if ds is None or ds.getRowCount() == 0:
            return None
        return {
            "set_version": int(ds.getValueAt(0, 0)),
            "values": _loads(ds.getValueAt(0, 1)),
        }

    # ---------- retention ----------

    def get_retention_config(self, tx=None):
        """
        Read retention configuration from `workflows.retention_config`.

        Args:
            tx (object|None): Optional transaction handle.

        Returns:
            dict: Retention settings with fallback defaults when row is missing.
        """
        sql = "SELECT time_threshold_hours, rows_threshold, global_timeout_hours FROM workflows.retention_config WHERE id = 1"
        ds = self.q(sql, [], tx=tx)
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

    def enforce_global_timeout(self, hours, tx=None):
        """
        Cancel active workflows older than `hours`.

        Args:
            hours (float|int|None): Age threshold in hours.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows updated.
        """
        if hours is None or hours == 0:
            return 0
        cutoff = now_ms() - long(float(hours) * 3600.0 * 1000.0)
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
        ms = now_ms()
        n = self.u(
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
            "evt=db.retention.global_timeout hours=%s cutoff=%s n=%s"
            % (hours, cutoff, n)
        )
        return n

    def delete_old_terminal(self, hours, tx=None):
        """
        Delete terminal workflows older than configured age, including child rows.

        Args:
            hours (float|int|None): Age threshold in hours.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows deleted across all affected tables.
        """
        if hours is None or hours == 0:
            return 0
        cutoff = now_ms() - long(float(hours) * 3600.0 * 1000.0)

        sel = """
        SELECT workflow_id
        FROM workflows.workflow_status
        WHERE status IN (?, ?, ?)
          AND COALESCE(completed_at_epoch_ms, updated_at_epoch_ms) < ?
        """
        ds = self.q(
            sel,
            [
                settings.STATUS_SUCCESS,
                settings.STATUS_ERROR,
                settings.STATUS_CANCELLED,
                cutoff,
            ],
            tx=tx,
        )
        ids = []
        for r in range(ds.getRowCount()):
            ids.append(str(ds.getValueAt(r, 0)))
        if not ids:
            return 0

        deleted = 0
        for wid in ids:
            deleted += self.u(
                "DELETE FROM workflows.operation_outputs WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_events WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_events_history WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.notifications WHERE destination_uuid = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.streams WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.stream_heads WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_status WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )

        log.info(
            "evt=db.retention.delete_old_terminal hours=%s cutoff=%s workflows=%s deleted_rows=%s"
            % (hours, cutoff, len(ids), deleted)
        )
        return deleted

    def enforce_rows_threshold(self, rows_threshold, tx=None):
        """
        If workflow_status row count exceeds threshold, delete oldest terminal workflows.

        Args:
            rows_threshold (int|long|None): Maximum allowed workflow_status rows.
            tx (object|None): Optional transaction handle.

        Returns:
            int: Number of rows deleted across all affected tables.
        """
        if rows_threshold is None or rows_threshold == 0:
            return 0
        total = self.scalar(
            "SELECT COUNT(*) FROM workflows.workflow_status", [], tx=tx, default=0
        )
        total = long(total)
        if total <= long(rows_threshold):
            return 0

        to_remove = total - long(rows_threshold)
        if to_remove <= 0:
            return 0

        sel = """
        SELECT workflow_id
        FROM workflows.workflow_status
        WHERE status IN (?, ?, ?)
        ORDER BY COALESCE(completed_at_epoch_ms, updated_at_epoch_ms) ASC
        LIMIT ?
        """
        ds = self.q(
            sel,
            [
                settings.STATUS_SUCCESS,
                settings.STATUS_ERROR,
                settings.STATUS_CANCELLED,
                long(to_remove),
            ],
            tx=tx,
        )
        ids = []
        for r in range(ds.getRowCount()):
            ids.append(str(ds.getValueAt(r, 0)))
        if not ids:
            return 0

        deleted = 0
        for wid in ids:
            deleted += self.u(
                "DELETE FROM workflows.operation_outputs WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_events WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_events_history WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.notifications WHERE destination_uuid = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.streams WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.stream_heads WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )
            deleted += self.u(
                "DELETE FROM workflows.workflow_status WHERE workflow_id = ?::uuid",
                [wid],
                tx=tx,
            )

        log.info(
            "evt=db.retention.enforce_rows_threshold threshold=%s total=%s removed=%s deleted_rows=%s"
            % (rows_threshold, total, len(ids), deleted)
        )
        return deleted

    # ---------- row utils ----------

    def _row_to_dict(self, ds, row_idx):
        """
        Convert one PyDataSet row into a plain dict keyed by column name.

        Args:
            ds (PyDataSet): Result dataset.
            row_idx (int): Row index.

        Returns:
            dict: Row dictionary.
        """
        col_names = ds.getColumnNames()
        out = {}
        for c in range(len(col_names)):
            k = col_names[c]
            v = ds.getValueAt(row_idx, c)
            out[k] = v
        return out
