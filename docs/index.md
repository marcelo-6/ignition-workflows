---
icon: lucide/book-open
---

# Ignition Workflows

A lightweight workflow orchestration library for [Ignition](http://ia.io), inspired by DBOS ideas but adapted to real Ignition constraints.

## Why this exists

If you have ever tried to scale long-running logic with regular event scripts, you already know the pain points:

- hard to track state over time
- hard to retry safely
- hard to do operator controls cleanly
- easy to tie up threads if you are not careful

This project gives you a cleaner structure:

- workflows for orchestration
- steps for side effects
- durable-ish state in Postgres
- operator controls (`HOLD`, `RESUME`, `STOP`, cancel)

## What you can do with it

- Queue work from Perspective, gateway scripts, or tag events.
- Keep status, outputs, events, and streams in Postgres.
- Run multiple workflows concurrently with queue + partition controls.
- Use maintenance mode for cleaner deployments and cutovers.

## Quick way in

- If you want setup first: start at [Getting Started](getting-started/getting-started.md).
- If you want real operator flows: jump to [Examples / Operations](examples/operations.md).
- If you want the UI map: use [UI Walkthrough](ui/workflows-console.md).
- If you want internals: read [Architecture](concepts/architecture.md) and [Concurrency + Lifecycle](concepts/concurrency-lifecycle.md).

## Quick snippets

=== "From a Perspective button"

    ```python title="onActionPerformed"
    def runAction(self, event):
        ret = exchange.workflows.api.service.start(
            workflowName="demo.commands_60s",
            inputs={"resolved": {"paramOne": "hello"}},
            queueName="default",
            priority=0,
        )
        print ret
    ```

=== "From a tag event (fast path)"

    ```python title="valueChanged"
    def valueChanged(tag, tagPath, previousValue, currentValue, initialChange, missedEvents):
        ack = exchange.workflows.api.service.enqueueInMemory(
            workflowName="demo.commands_60s",
            inputs={"resolved": {"paramOne": "from-tag"}},
            queueName="default",
        )
        print ack
    ```

=== "Control a running workflow"

    ```python
    wid = "<workflow-id>"
    print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="HOLD", reason="operator hold")
    print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="RESUME")
    print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="STOP", reason="operator stop")
    print exchange.workflows.api.service.cancel(workflowId=wid, reason="manual cancel")
    ```

## Limitations

This project is not trying to replicate DBOS 1:1 and is not trying to replace Ignition SFC.

Current roadmap and caveats are in [Roadmap](roadmap.md).
