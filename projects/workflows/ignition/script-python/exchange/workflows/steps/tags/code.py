# exchange/workflows/steps/tags.py
"""
First-class tag steps (recommended patterns).

These are guidelines and utilities; users can write their own steps as needed.
"""

import time

from exchange.workflows.engine.runtime import step

log = system.util.getLogger("exchange." + system.util.getProjectName().lower() +".steps.tags")

@step(name="tags.read")
def read(path):
    """
    Read a single tag.
            Returns value.

    Args:
        path (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    qv = system.tag.readBlocking([path])[0]
    return {"value": qv.value, "quality": str(qv.quality), "timestamp": str(qv.timestamp)}


@step(name="tags.write")
def write(path, value):
    """
    Fire-and-forget tag write. Returns write quality info.

    Args:
        path (object): Input value for this call.
        value (object): Value payload to persist or publish.

    Returns:
        dict: Structured payload with results or status details.
    """
    qs = system.tag.writeBlocking([path], [value])[0]
    return {"quality": str(qs)}


@step(name="tags.write_confirm")
def write_confirm(path, value, confirm_path=None, timeout_ms=10000, poll_ms=250):
    """
    Write and confirm (bounded).
    
            confirm_path defaults to same path.
            Confirms by checking confirm_path equals value.

    Args:
        path (object): Input value for this call.
        value (object): Value payload to persist or publish.
        confirm_path (object): Input value for this call.
        timeout_ms (object): Time value in milliseconds.
        poll_ms (object): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    if confirm_path is None:
        confirm_path = path

    qs = system.tag.writeBlocking([path], [value])[0]
    start = system.date.toMillis(system.date.now())
    deadline = start + long(timeout_ms)

    while system.date.toMillis(system.date.now()) < deadline:
        qv = system.tag.readBlocking([confirm_path])[0]
        if qv.value == value:
            return {"ok": True, "write_quality": str(qs), "confirmed": True}
        time.sleep(float(poll_ms) / 1000.0)

    return {"ok": False, "write_quality": str(qs), "confirmed": False, "timeout_ms": timeout_ms}