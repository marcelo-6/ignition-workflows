# exchange/workflows/steps/wait.py
"""
Generic wait helpers.

Keep these bounded and chunked to avoid blocking responsiveness.
"""

import time

from exchange.workflows.engine.runtime import step

log = system.util.getLogger("exchange." + system.util.getProjectName().lower() +".steps.wait")

@step(name="wait.until_tag_equals")
def until_tag_equals(path, expected, timeout_ms=10000, poll_ms=250):
    """
    Poll a tag until its value equals expected.

    Args:
        path (object): Input value for this call.
        expected (object): Input value for this call.
        timeout_ms (object): Time value in milliseconds.
        poll_ms (object): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    start = system.date.toMillis(system.date.now())
    deadline = start + long(timeout_ms)

    while system.date.toMillis(system.date.now()) < deadline:
        qv = system.tag.readBlocking([path])[0]
        if qv.value == expected:
            return {"ok": True, "value": qv.value, "quality": str(qv.quality)}
        time.sleep(float(poll_ms) / 1000.0)

    qv = system.tag.readBlocking([path])[0]
    return {"ok": False, "value": qv.value, "quality": str(qv.quality), "timeout_ms": timeout_ms}