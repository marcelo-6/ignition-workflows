---
icon: lucide/rocket
---

# Getting Started

This is the fastest path to get workflows running.

## Prerequisites

- Ignition Gateway project imported (`projects/workflows`).
- Postgres reachable from Ignition.
- Ignition DB connection named `WorkflowsDB` (default).
- Gateway timer script enabled at `ignition/timer/tick`.

## Create schema

### Use named query

- `Exchange/Workflows/Schemas/Create`

### Or use the admin screen

- location

## Start a workflow

```python
resp = exchange.workflows.api.service.start(
    workflowName="demo.commands_60s",
    inputs={"resolved": {"paramOne": "hello"}},
    queueName="default",
    priority=0,
)
print resp
```

The workflow id is `resp["data"]["workflowId"]`.

### Send commands to a run

```python
wid = resp["data"]["workflowId"]
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="HOLD", reason="operator hold")
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="RESUME")
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="STOP", reason="stop requested")
```

## Example Sequence Diagram

```mermaid
sequenceDiagram
    participant G as Gateway Startup
    participant T as Timer Script
    participant S as service.start
    participant R as WorkflowsRuntime
    participant P as Postgres

    G->>R: getWorkflows() to load app modules
    S->>R: enqueue(workflowName, inputs)
    R->>P: INSERT workflow_status (ENQUEUED)

    loop every timer tick
      T->>R: dispatch()
      R->>P: claim ENQUEUED -> PENDING
      R->>R: execute workflow + steps
      R->>P: persist events/streams/outputs/status
    end
```
