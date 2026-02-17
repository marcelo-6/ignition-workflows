# exchange/workflows/apps/examples/workflows.py
"""Example workflows and steps for demos and manual validation."""


from exchange.workflows.engine.runtime import workflow
from exchange.workflows.engine.instance import getWorkflows

from exchange.workflows.steps.sleep import sleep_chunked

log = system.util.getLogger(
    "exchange." + system.util.getProjectName().lower() + ".example-workflows"
)


@workflow(name="demo.one_minute_progress")
def wf_demo_one_minute_progress(timeout_seconds=120, chunk_sec=5):
    """
    Demo workflow:
            - Runs for ~60 seconds in chunked sleeps
            - Emits progress (stream log + event)
            - Uses exactly 2 steps:
                1) tags.write
                2) sleep.sleep_chunked

    Args:
        timeout_seconds (float): Timeout in seconds for workflow execution.
        chunk_sec (object): Time value in seconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    rt = getWorkflows()

    total_sec = 60
    chunk_sec = int(chunk_sec)
    if chunk_sec <= 0:
        chunk_sec = 5

    loops = int(total_sec / chunk_sec)
    if loops <= 0:
        loops = 1

    rt.progress(
        "Starting demo.one_minute_progress",
        extra={
            "total_sec": total_sec,
            "chunk_sec": chunk_sec,
            "loops": loops,
        },
    )

    for i in range(1, loops + 1):
        # cooperative cancel check
        rt.checkCancelled()

        pct = int((float(i) / float(loops)) * 100.0)

        # Progress events/logs (NOT steps; safe + fast)
        rt.progress("Progress %d/%d" % (i, loops), extra={"pct": pct})

        # Step 2: chunked sleep to stay responsive to HOLD/STOP/CANCEL patterns
        sleep_chunked(total_ms=chunk_sec * 1000, chunk_ms=250)

    rt.progress("Completed demo.one_minute_progress")

    return {"ok": True, "duration_sec": total_sec}


# exchange/workflows/apps/demo_commands.py
"""
Command playground workflows.

Goal:
- Give UI buttons something safe to drive:
  HOLD / RESUME / CANCEL / RESET
- Show state transitions via workflow_events + stream log.
"""

import time

from exchange.workflows.engine.runtime import workflow, step
from exchange.workflows.engine.instance import getWorkflows


# ----------------------------
# Steps (2 tiny steps)
# ----------------------------


@step(
    name="demo.cmd.set_state",
    options={"retries_allowed": True, "max_attempts": 3, "interval_seconds": 10},
)
def set_state(state, note=None):
    """
    Side-effect step: update phase.state and log stream.

    Args:
        state (str): Input value for this call.
        note (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    rt = getWorkflows()
    rt.setEvent("phase.state", str(state))
    rt.writeStream("log", {"event": "state", "state": str(state), "note": note})
    return {"state": state, "note": note}


@step(name="demo.cmd.sleep_chunked")
def sleep_chunked(total_ms=5000, chunk_ms=250):
    """
    Side-effect step: chunked sleep so HOLD/CANCEL is responsive.

    Args:
        total_ms (object): Time value in milliseconds.
        chunk_ms (int): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    rt = getWorkflows()
    total_ms = int(total_ms)
    chunk_ms = max(50, int(chunk_ms))
    slept = 0
    chunks = 0

    while slept < total_ms:
        rt.checkCancelled()  # cooperative cancel

        # emit a tiny progress event so UI has something to show
        rt.setEvent("phase.progress_ms", slept)

        time.sleep(float(chunk_ms) / 1000.0)
        slept += chunk_ms
        chunks += 1

    rt.setEvent("phase.progress_ms", total_ms)
    return {"slept_ms": total_ms, "chunk_ms": chunk_ms, "chunks": chunks}


# ----------------------------
# Workflow: command playground
# ----------------------------


@workflow(name="demo.commands_60s")
def commands_60s(templateInfo=None, paramOne=None, paramTwo=None, paramThree=None):
    """
    Runs ~60s (default), repeatedly sleeping in chunks and responding to commands.

            Commands (sent via mailbox topic="cmd"):
              - HOLD: enters HELD loop until RESUME
              - RESUME: exits HELD
              - CANCEL: not a mailbox cmd; use rt.cancelWorkflow from UI (DB status)
              - RESET: soft reset (clears some events, then continues)

    Args:
        templateInfo (object): Input value for this call.
        paramOne (object): Input value for this call.
        paramTwo (object): Input value for this call.
        paramThree (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    rt = getWorkflows()
    duration_s = 60
    tick_ms = 500
    duration_s = int(duration_s)
    tick_ms = max(100, int(tick_ms))

    #    set_state("RUNNING", note="workflow started")
    rt.progress("RUNNING workflow started")

    rt.writeStream("log", {"paramOne": paramOne})
    rt.setEvent("phase.started_at_ms", system.date.toMillis(system.date.now()))

    deadline = system.date.addSeconds(system.date.now(), duration_s)
    loop_i = 0

    while system.date.isBefore(system.date.now(), deadline):
        rt.checkCancelled()

        # Poll mailbox for commands (non-blocking)
        cmd_info = rt.recvCommand(
            currentState="RUNNING", topic="cmd", timeoutSeconds=30, maxMessages=8
        )
        log.info("command info = {}".format(cmd_info))
        if cmd_info:
            cmd = str(cmd_info.get("cmd", "")).upper()

            if cmd == "HOLD":
                #                set_state("HELD", note="received HOLD")
                rt.progress("HELD received HOLD")
                # HELD loop: only RESUME (or STOP if you add it) will exit
                while True:
                    rt.checkCancelled()
                    held_cmd = rt.recvCommand(
                        currentState="HELD",
                        topic="cmd",
                        timeoutSeconds=0.2,
                        maxMessages=8,
                    )
                    if held_cmd:
                        c2 = str(held_cmd.get("cmd", "")).upper()
                        if c2 == "RESUME":
                            rt.progress("RUNNING received RESUME")
                            #                            set_state("RUNNING", note="received RESUME")
                            break
                        if c2 == "RESET":
                            # allow reset while held too
                            rt.setEvent("phase.progress_ms", 0)
                            rt.writeStream(
                                "log", {"event": "reset", "note": "reset while held"}
                            )
                            rt.progress("HELD reset done; still held")
            #                            set_state("HELD", note="reset done; still held")

            elif cmd == "RESUME":
                # If someone hits RESUME while not held, just log it.
                rt.writeStream(
                    "log", {"event": "cmd_ignored", "cmd": "RESUME", "note": "not held"}
                )
            elif cmd == "RESET":
                # Soft reset: clear a few UI-facing events; keep workflow running
                rt.setEvent("phase.progress_ms", 0)
                rt.setEvent(
                    "phase.last_reset_ms", system.date.toMillis(system.date.now())
                )
                rt.writeStream("log", {"event": "reset", "note": "soft reset"})
                # state stays RUNNING
            else:
                rt.writeStream("log", {"event": "cmd_unknown", "cmd": cmd})

        # Do “work” in small chunks so UI feels snappy
        loop_i += 1
        rt.setEvent("phase.loop_i", loop_i)
        sleep_chunked(total_ms=tick_ms, chunk_ms=min(250, tick_ms))

    #    set_state("COMPLETE", note="finished normal duration")
    rt.progress("COMPLETE finished normal duration")
    return {"ok": True, "duration_s": duration_s, "loops": loop_i}
