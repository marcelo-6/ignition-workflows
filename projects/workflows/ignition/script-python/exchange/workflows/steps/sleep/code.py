# exchange/workflows/steps/sleep.py
"""
Sleep steps.

sleep.chunked is important for command responsiveness: workflows can call it
between safe points, or you can implement "wait loops" using chunked sleep.
"""

import time

from exchange.workflows.engine.runtime import step

log = system.util.getLogger("exchange." + system.util.getProjectName().lower() +".steps.sleep")

@step(name="sleep.ms")
def sleep_ms(ms):
    """
    Sleep for ms (single shot).

    Args:
        ms (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    time.sleep(float(ms) / 1000.0)
    return {"slept_ms": ms}


@step(name="sleep.chunked")
def sleep_chunked(total_ms, chunk_ms=250):
    """
    Sleep in small chunks. Returns how many chunks executed.

    Args:
        total_ms (object): Time value in milliseconds.
        chunk_ms (int): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    total_ms = long(total_ms)
    chunk_ms = long(chunk_ms)

    remaining = total_ms
    chunks = 0
    while remaining > 0:
        this_ms = chunk_ms if remaining > chunk_ms else remaining
        time.sleep(float(this_ms) / 1000.0)
        remaining -= this_ms
        chunks += 1
    return {"slept_ms": total_ms, "chunks": chunks, "chunk_ms": chunk_ms}