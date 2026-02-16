# exchange/workflows/apps/examples/workflows.py
"""
Sleep step helpers.

These are cooperative-friendly:
- break long waits into chunks
- check cancellation between chunks
"""

import time

from exchange.workflows.engine.runtime import step
from exchange.workflows.engine.instance import getWorkflows


@step(name="sleep.sleep_chunked", options={"retries_allowed": False, "max_attempts": 1})
def sleep_chunked(total_ms, chunk_ms=250):
    """
    Sleep for total_ms, but in chunk_ms slices, checking cancellation between chunks.

    Args:
        total_ms (object): Time value in milliseconds.
        chunk_ms (int): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    rt = getWorkflows()

    try:
        total_ms = long(total_ms)
    except:
        total_ms = 0

    try:
        chunk_ms = long(chunk_ms)
    except:
        chunk_ms = 250

    if total_ms <= 0:
        return {"total_ms": 0, "chunk_ms": chunk_ms}

    if chunk_ms <= 0:
        chunk_ms = 250

    remaining = total_ms
    while remaining > 0:
        if rt is not None:
            rt.checkCancelled()

        this_ms = chunk_ms if remaining > chunk_ms else remaining
        time.sleep(float(this_ms) / 1000.0)
        remaining -= this_ms

    return {"total_ms": total_ms, "chunk_ms": chunk_ms}
