# exchange/workflows/util/date.py
def nowMs():
    """
    Return current epoch time in milliseconds.

    Args:
        None.

    Returns:
        long: Current epoch milliseconds.
    """
    return system.date.toMillis(system.date.now())