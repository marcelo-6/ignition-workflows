# exchange/workflows/settings.py
"""
Shared settings for the workflows package.

This module keeps common constants in one place so API/admin/runtime code does not
sprinkle hard-coded defaults across files.

"""

DBNAME_DEFAULT = "WorkflowsDB"
QUEUE_DEFAULT = "default"
KERNEL_KEY = "exchange.workflows.kernel.v1"
MAX_CLAIM_DEFAULT = 10
FAST_FLUSH_MAX_ITEMS = 200
FAST_FLUSH_MAX_MS = 25
FAST_QUEUE_MAX_SIZE = 50000
FAST_FLUSH_FAILURE_MAX_RETRIES = 3
FAST_DEAD_LETTER_MAX_SIZE = 1000

STATUS_ENQUEUED = "ENQUEUED"
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_CANCELLED = "CANCELLED"
STATUS_SUCCESS = "SUCCESS"
STATUS_ERROR = "ERROR"

WORKFLOW_TERMINAL_STATUSES = (
    STATUS_CANCELLED,
    STATUS_SUCCESS,
    STATUS_ERROR,
)

STEP_STATUS_STARTED = "STARTED"


def getLogPrefix():
    """
    Build the concrete workflows log prefix for the current project.

    Returns:
            str: Prefix like `exchange.<project>.workflows`, except when project is
                 literally `workflows`, in which case return `exchange.workflows`.
    """
    return "exchange.%s.workflows" % system.util.getProjectName().lower()


def getLoggerName(component):
    """
    Return a fully qualified logger name for a workflows component.

    Args:
            component (str): Component suffix such as `api.service` or `engine.runtime`.

    Returns:
            str: Logger name with standardized prefix.
    """
    comp = str(component or "").strip(".")
    if not comp:
        return getLogPrefix()
    return "%s.%s" % (getLogPrefix(), comp)


def resolveDbName(dbName=None):
    """
    Resolve a caller-provided database name to a concrete defaulted value.

    Args:
            dbName (str|None): Optional Ignition DB connection name.

    Returns:
            str: Caller value when provided, otherwise DBNAME_DEFAULT.
    """
    if dbName:
        return dbName
    return DBNAME_DEFAULT


def resolveQueueName(queueName=None):
    """
    Resolve a caller-provided queue name to a concrete defaulted value.

    Args:
            queueName (str|None): Optional queue name.

    Returns:
            str: Caller value when provided, otherwise QUEUE_DEFAULT.
    """
    if queueName:
        return queueName
    return QUEUE_DEFAULT
