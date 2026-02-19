# exchange/workflows/engine/instance.py
"""
Kernel access helpers for workflows runtime facades.

Purpose:
- Keep one persistent Java-only kernel in gateway globals.
- Never persist custom Jython runtime instances across reloads.
- Create ephemeral `WorkflowsRuntime` facades on demand.

Key entry points:
- getWorkflows(): return runtime facade for a DB connection.
- shutdownRuntime(): stop executors and delete kernel key.
"""

from java.util import Collections, UUID
from java.util.concurrent import (
    ConcurrentHashMap,
    ConcurrentLinkedQueue,
    Executors,
    TimeUnit,
)
from java.util.concurrent.atomic import AtomicBoolean, AtomicInteger, AtomicLong

from exchange.workflows import settings
from exchange.workflows.engine.runtime import WorkflowsRuntime

_KERNEL_KEY = settings.KERNEL_KEY
_OLD_RUNTIME_KEY = "exchange.workflows.runtime.instance.v1"
_DEFAULT_WORKFLOW_WORKERS = 4
_DEFAULT_STEP_WORKERS = 8

log = system.util.getLogger(settings.getLoggerName("engine.instance"))

_apps_loaded = False


def _getStoreMap():
    """
    Get the persistent map used to store kernel state.

    Args:
            None.

    Returns:
            tuple: `(store, storeType)` where `store` is mutable and persistent for the gateway JVM.
    """
    try:
        if hasattr(system.util, "globalVarMap"):
            return system.util.globalVarMap(_KERNEL_KEY), "globalVarMap"
    except Exception as e:
        log.warn(
            "system.util.globalVarMap unavailable/failed; falling back to system.util.getGlobals(): %s"
            % e
        )
    try:
        return system.util.getGlobals(), "globals"
    except Exception as e:
        log.error("No persistent store available for workflows kernel: %s" % e)
        return None, "none"


def _getStore(store, key, default=None):
    """
    Read a value from the persistent store with dict/map compatibility.

    Args:
            store (object): Persistent map handle from `_getStoreMap()`.
            key (str): Kernel key to read.
            default (object): Fallback when key is missing.

    Returns:
            object: Stored value when present; otherwise `default`.
    """
    if store is None:
        return default
    try:
        value = store.get(key)
        if value is None:
            return default
        return value
    except:
        pass
    try:
        return store[key]
    except:
        return default


def _setStore(store, key, value):
    """
    Write a value to the persistent store with dict/map compatibility.

    Args:
            store (object): Persistent map handle from `_getStoreMap()`.
            key (str): Kernel key to write.
            value (object): Value to persist.

    Returns:
            None: This helper mutates the store in place.
    """
    try:
        store[key] = value
        return
    except Exception as e:
        log.error("Failed to persist workflows kernel key %s: %s" % (key, e))
        # pass
    try:
        store.put(key, value)
    except Exception as e:
        log.error("Failed to persist workflows kernel key %s: %s" % (key, e))
        raise


def _delStore(store, key):
    """
    Delete a key from the persistent store with dict/map compatibility.

    Args:
            store (object): Persistent map handle from `_getStoreMap()`.
            key (str): Kernel key to remove.

    Returns:
            None: This helper mutates the store in place.
    """
    try:
        del store[key]
        return
    except:
        pass
    try:
        store.remove(key)
    except:
        pass


def _newJavaSet():
    """
    Create a concurrent Java set backed by `ConcurrentHashMap`.

    Args:
            None.

    Returns:
            java.util.Set: Thread-safe set compatible with persisted kernel usage.
    """
    return Collections.newSetFromMap(ConcurrentHashMap())


def _makeExecutorId():
    """
    Build a gateway-unique executor identifier for claims and logs.

    Args:
            None.

    Returns:
            str: Identifier shaped like `<gatewayName>:<uuid>`.
    """
    try:
        gatewayName = system.tag.readBlocking(tagPaths=["[System]Gateway/SystemName"])[
            0
        ].value
    except:
        gatewayName = "gateway"
    return "%s:%s" % (gatewayName, str(UUID.randomUUID()))


def _createKernel(
    workflowWorkers=_DEFAULT_WORKFLOW_WORKERS, stepWorkers=_DEFAULT_STEP_WORKERS
):
    """
    Create a Java-only kernel map with executors, counters, and in-memory queue state.

    Args:
            workflowWorkers (int): Workflow executor worker count.
            stepWorkers (int): Step executor worker count.

    Returns:
            java.util.concurrent.ConcurrentHashMap: Initialized kernel object persisted in globals.
    """
    kernel = ConcurrentHashMap()
    kernel.put("kernelVersion", "v1")
    kernel.put("executorId", _makeExecutorId())
    kernel.put("generation", AtomicLong(1))
    kernel.put("maintenanceEnabled", AtomicBoolean(False))
    kernel.put("maintenanceMode", "drain")
    kernel.put("maintenanceReason", "")
    kernel.put("inFlightWorkflows", AtomicInteger(0))
    kernel.put("activePartitions", _newJavaSet())
    kernel.put("activeWorkflows", _newJavaSet())
    kernel.put("wfExecutor", Executors.newFixedThreadPool(int(workflowWorkers)))
    kernel.put("stepExecutor", Executors.newFixedThreadPool(int(stepWorkers)))
    kernel.put("inMemoryQueue", ConcurrentLinkedQueue())
    kernel.put(
        "inMemoryQueueMaxDepth", AtomicInteger(int(settings.FAST_QUEUE_MAX_SIZE))
    )
    kernel.put("inMemoryQueueOverflowCount", AtomicLong(0))
    kernel.put("inMemoryQueueLastOverflowAtMs", AtomicLong(0))
    kernel.put("inMemoryQueueFlushFailureCount", AtomicLong(0))
    kernel.put("inMemoryQueueDeadLetterQueue", ConcurrentLinkedQueue())
    kernel.put("inMemoryQueueDeadLetterCount", AtomicLong(0))
    kernel.put("inMemoryQueueOverflowMode", "reject")
    kernel.put("inMemoryQueueAcceptDuringMaintenance", AtomicBoolean(False))
    kernel.put("inMemoryQueueFlushDuringMaintenance", AtomicBoolean(False))
    kernel.put("lastSwapAtMs", AtomicLong(0))
    kernel.put("workflowWorkers", AtomicInteger(int(workflowWorkers)))
    kernel.put("stepWorkers", AtomicInteger(int(stepWorkers)))
    return kernel


def _ensureKernel(store):
    """
    Load the persistent kernel or create it when missing.

    Args:
            store (object): Persistent map handle from `_getStoreMap()`.

    Returns:
            java.util.concurrent.ConcurrentHashMap: Existing or newly created kernel.
    """
    kernel = _getStore(store, _KERNEL_KEY)
    if kernel is not None:
        return kernel
    kernel = _createKernel()
    _setStore(store, _KERNEL_KEY, kernel)
    _delStore(store, _OLD_RUNTIME_KEY)
    log.info(
        "Created workflows kernel %s executor_id=%s"
        % (str(kernel.get("kernelVersion")), str(kernel.get("executorId")))
    )
    return kernel


def _ensureWorkflowsLoaded():
    """
    Load workflow/step registration modules once per interpreter session.

    Args:
            None.

    Returns:
            None: Imports are executed for side effects only.
    """
    global _apps_loaded
    if _apps_loaded:
        return
    try:
        from exchange.workflows.apps import __init__ as workflowsApps

        workflowsApps.load()
        _apps_loaded = True
    except Exception as e:
        log.error("_ensureWorkflowsLoaded failed: %s" % str(e))
        raise


def getWorkflows(dbName=None):
    """
    Return an ephemeral `WorkflowsRuntime` facade backed by the persistent kernel.

    Args:
            dbName (str|None): Compatibility DB name argument.
            dbName (str|None): Preferred camelCase DB name argument.

    Returns:
            WorkflowsRuntime: Runtime facade bound to the requested DB connection.
    """
    resolvedDbName = settings.resolveDbName(dbName)
    store, _ = _getStoreMap()
    if store is None:
        raise Exception("No persistent store available for workflows kernel")
    kernel = _ensureKernel(store)
    _ensureWorkflowsLoaded()
    return WorkflowsRuntime(dbName=resolvedDbName, kernel=kernel)


def shutdownRuntime(wait_sec=5):
    """
    Shutdown kernel executors and remove the persisted kernel key.

    Args:
            wait_sec (int): Maximum seconds to wait for executor termination.

    Returns:
            bool: `True` when a kernel existed and was removed; otherwise `False`.
    """
    store, _ = _getStoreMap()
    if store is None:
        return False
    kernel = _getStore(store, _KERNEL_KEY)
    if kernel is None:
        _delStore(store, _OLD_RUNTIME_KEY)
        log.info("shutdownRuntime: workflows kernel already non-existent")
        return False

    try:
        wf = kernel.get("wfExecutor")
        if wf is not None:
            wf.shutdown()
    except:
        pass
    try:
        st = kernel.get("stepExecutor")
        if st is not None:
            st.shutdown()
    except:
        pass
    try:
        wf = kernel.get("wfExecutor")
        if wf is not None:
            wf.awaitTermination(int(wait_sec), TimeUnit.SECONDS)
    except:
        pass
    try:
        st = kernel.get("stepExecutor")
        if st is not None:
            st.awaitTermination(int(wait_sec), TimeUnit.SECONDS)
    except:
        pass

    _delStore(store, _KERNEL_KEY)
    _delStore(store, _OLD_RUNTIME_KEY)
    log.info("shutdownRuntime: workflows kernel deleted")
    return True
