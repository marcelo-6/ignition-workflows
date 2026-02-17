# exchange/workflows/steps/db.py
"""
DB steps for application DB usage (NOT the Workflows engine DB).

These are intentionally generic wrappers. If users need more control, they can
write their own steps.
"""

from exchange.workflows.engine.runtime import step

log = system.util.getLogger(
    "exchange." + system.util.getProjectName().lower() + ".steps.db"
)


@step(name="db.query")
def query(dbName, sql, args=None):
    """
    Run a prepared query against the specified Ignition DB connection.
            Returns list-of-dicts.

    Args:
        dbName (str): Ignition database connection name.
        sql (object): Input value for this call.
        args (str): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    args = args or []
    ds = system.db.runPrepQuery(sql, args, dbName)
    out = []
    cols = ds.getColumnNames()
    for r in range(ds.getRowCount()):
        row = {}
        for c in range(len(cols)):
            row[cols[c]] = ds.getValueAt(r, c)
        out.append(row)
    return out


@step(name="db.update")
def update(dbName, sql, args=None):
    """
    Run a prepared update against the specified Ignition DB connection.
            Returns affected row count.

    Args:
        dbName (str): Ignition database connection name.
        sql (object): Input value for this call.
        args (str): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    args = args or []
    n = system.db.runPrepUpdate(sql, args, dbName)
    return {"rows_affected": int(n)}
