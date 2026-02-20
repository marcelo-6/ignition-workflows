---
icon: lucide/play-circle
---

# Operations Example (End-to-End)

This page is the practical "show me the flow" version.

## Scenario

You want to run a workflow, watch it live, send operator commands, and verify what got persisted.

## Demo data

Use this so screenshots and docs stay sanitized:

- `workflowName`: `demo.commands_60s`
- `queueName`: `default`
- `partitionKey`: `line-1/reactor-a`
- `paramOne`: `BATCH-001`
- `paramTwo`: `AUTO`

## Path A: Start from UI

1. Open `/`.
2. Select `demo.commands_60s`.
3. Pick template `default` (if available).
4. Set demo values (`BATCH-001`, `AUTO`).
5. Press **Start**.
6. Open `/runs` and filter by workflow name.
7. Open the matching run details page.

## Path B: Start from script

```python
resp = exchange.workflows.api.service.start(
    workflowName="demo.commands_60s",
    inputs={"resolved": {"paramOne": "BATCH-001", "paramTwo": "AUTO"}},
    queueName="default",
    partitionKey="line-1/reactor-a",
    timeoutSeconds=180,
)
print resp
```

## Control loop example

Once you have a workflow id (`wid`):

```python
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="HOLD", reason="operator hold")
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="RESUME")
print exchange.workflows.api.service.sendCommand(workflowId=wid, cmd="STOP", reason="operator stop")
```

What you should observe:

- status and phase events update in details view
- stream logs continue appending while run is active
- steps page shows timeline and step outputs

## Verify persistence quickly

Check from UI:

- `/runs/<uuid>` -> details/events/output/error panels
- `/runs/<uuid>/steps` -> step table + timeline
- `/queues` -> status counts

Check from DB (optional):

- `workflows.workflow_status`
- `workflows.operation_outputs`
- `workflows.workflow_events`
- `workflows.streams`

## Related docs

- [UI Walkthrough](../ui/workflows-console.md)
- [Service API](../api-reference/api-service.md)
- [Troubleshooting + Observability](../operations/troubleshooting-observability.md)
