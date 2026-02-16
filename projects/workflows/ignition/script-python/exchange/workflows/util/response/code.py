# exchange/workflows/util/response.py
"""
Standard response envelope helpers for public workflows APIs.

Use these at API/admin boundaries so every entrypoint returns:
{ok, code, message, data?, meta?}
"""
from exchange.workflows import settings


def _now_ms():
    """
    Return current epoch milliseconds.

    Returns:
            long: Current epoch milliseconds.
    """
    return system.date.toMillis(system.date.now())


def _normalize_meta(meta=None, startedMs=None):
    """
    Build a normalized metadata dictionary with timestamp and optional duration.

    Args:
            meta (dict|None): Optional metadata fields supplied by caller.
            startedMs (int|long|None): Optional start time for duration calculation.

    Returns:
            dict: Metadata object with `tsMs` and optional `durationMs`.
    """
    out = {}
    if isinstance(meta, dict):
        out.update(meta)
    now = _now_ms()
    out["tsMs"] = now
    if startedMs is not None:
        try:
            out["durationMs"] = max(0, long(now - long(startedMs)))
        except:
            pass
    return out


def _merge_compat_fields(envelope):
    """
    Copy `data` fields to top-level keys for legacy callers.

    This keeps the standard envelope while preserving backward compatibility for
    scripts that still read `result["someDataKey"]` directly.

    Args:
            envelope (dict): Response envelope produced by `makeOk`/`makeErr`.

    Returns:
            dict: Envelope with non-conflicting data keys mirrored at top level.
    """
    data = envelope.get("data")
    if not isinstance(data, dict):
        return envelope
    for key, value in data.items():
        if key in envelope:
            continue
        envelope[key] = value
    return envelope


def makeOk(code, message, data=None, meta=None, startedMs=None):
    """
    Create a successful API envelope.

    Args:
            code (str): Stable success code used by UI logic.
            message (str): Short human-readable summary.
            data (dict|None): Optional payload.
            meta (dict|None): Optional metadata fields.
            startedMs (int|long|None): Optional start time for automatic `durationMs`.

    Returns:
            dict: Standard envelope with `ok=True`.
    """
    envelope = {
        "ok": True,
        "code": str(code or "OK"),
        "message": str(message or "OK"),
        "data": data if isinstance(data, dict) else {},
        "meta": _normalize_meta(meta=meta, startedMs=startedMs),
    }

    return _merge_compat_fields(envelope)


def makeErr(code, message, data=None, meta=None, startedMs=None):
    """
    Create an error API envelope.

    Args:
            code (str): Stable error code used by UI logic.
            message (str): Short human-readable summary.
            data (dict|None): Optional error details.
            meta (dict|None): Optional metadata fields.
            startedMs (int|long|None): Optional start time for automatic `durationMs`.

    Returns:
            dict: Standard envelope with `ok=False`.
    """
    envelope = {
        "ok": False,
        "code": str(code or "ERROR"),
        "message": str(message or "Error"),
        "data": data if isinstance(data, dict) else {},
        "meta": _normalize_meta(meta=meta, startedMs=startedMs),
    }

    return _merge_compat_fields(envelope)
