# exchange/workflows/isa88/model.py
"""
ISA-88-ish minimal state + command model (ABORT removed).

This file provides constants and small helpers for workflows to remain consistent.
"""

# States
ST_IDLE = "IDLE"
ST_STARTING = "STARTING"
ST_RUNNING = "RUNNING"
ST_HOLDING = "HOLDING"
ST_HELD = "HELD"
ST_RESUMING = "RESUMING"
ST_STOPPING = "STOPPING"
ST_STOPPED = "STOPPED"
ST_COMPLETE = "COMPLETE"
ST_ERROR = "ERROR"

ALL_STATES = (
    ST_IDLE,
    ST_STARTING,
    ST_RUNNING,
    ST_HOLDING,
    ST_HELD,
    ST_RESUMING,
    ST_STOPPING,
    ST_STOPPED,
    ST_COMPLETE,
    ST_ERROR,
)

# Commands
CMD_HOLD = "HOLD"
CMD_RESUME = "RESUME"
CMD_STOP = "STOP"
CMD_RESET = "RESET"

ALL_COMMANDS = (CMD_HOLD, CMD_RESUME, CMD_STOP, CMD_RESET)

# Priority: STOP > HOLD > RESUME (RESET is only relevant in terminal/faulted)
CMD_PRIORITY = {CMD_STOP: 2, CMD_HOLD: 1, CMD_RESUME: 0, CMD_RESET: -1}


def allowedInState(cmd, state):
    """
    Return True when a command is allowed for the supplied workflow state.

    Args:
        cmd (object): Command keyword or payload to send/process.
        state (str): Input value for this call.

    Returns:
        bool: True/False result for this operation.
    """
    cmd = (cmd or "").upper()
    state = (state or "").upper()

    if cmd == CMD_STOP:
        return True
    if cmd == CMD_HOLD:
        return state in (ST_STARTING, ST_RUNNING, ST_RESUMING)
    if cmd == CMD_RESUME:
        return state == ST_HELD
    if cmd == CMD_RESET:
        return state in (ST_STOPPED, ST_ERROR)
    return False


def normalizeCommand(messageOrCmd):
    """
    Normalize a raw command string or mailbox message to a known command value.

    Args:
        message_or_cmd (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    cmd = ""
    if isinstance(messageOrCmd, dict):
        cmd = str(messageOrCmd.get("cmd", "")).upper()
    else:
        cmd = str(messageOrCmd or "").upper()
    if cmd in ALL_COMMANDS:
        return cmd
    return None


def commandPriority(cmd):
    """
    Return command priority where higher values mean higher precedence.

    Args:
        cmd (object): Command keyword or payload to send/process.

    Returns:
        object: Result object returned by this call.
    """
    c = normalizeCommand(cmd)
    if c is None:
        return -999
    return int(CMD_PRIORITY.get(c, -999))


def pickHighestPriority(messages, state=None):
    """
    Pick the highest-priority allowed command from a batch of mailbox messages.

            This keeps command arbitration centralized so workflows do not duplicate
            STOP/HOLD/RESUME/RESET checks in slightly different ways.

    Args:
        messages (object): Input value for this call.
        state (str): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    if not messages:
        return None

    best = None
    best_prio = -999
    for msg in messages:
        cmd = normalizeCommand(msg)
        if cmd is None:
            continue
        allowed = True
        if state is not None:
            allowed = allowedInState(cmd, state)
        if not allowed:
            continue
        prio = commandPriority(cmd)
        if best is None or prio > best_prio:
            best = {"cmd": cmd, "message": msg, "priority": prio}
            best_prio = prio
    return best
