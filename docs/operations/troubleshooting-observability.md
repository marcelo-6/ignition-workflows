---
icon: lucide/stethoscope
---

# Troubleshooting + Observability

This is the page to use during incidents or "why is this stuck?" moments.

## First-pass triage

1. Open `/admin` and check maintenance status.
2. Open `/queues` and see which statuses are climbing.
3. Open `/runs` and locate a representative stuck/failed run.
4. Open `/runs/:uuid` and inspect status, error payload, events, and stream log.

If you do only those four, you usually get enough signal to decide next action.

## What to watch in UI

### `/admin`

- maintenance enabled/mode/reason
- generation counter
- workflow executor active count
- step executor active count
- in-memory queue depth + overflow/failure counters

### `/queues`

- queue-level status counts (`ENQUEUED`, `PENDING`, `RUNNING`, terminal states)

### `/runs/:uuid`

- current status
- timestamps (`created/started/updated/completed`)
- input/output/error payloads
- events panel
- stream log popup

## Common symptoms

## Symptom: runs stuck in `ENQUEUED`

Likely causes:

- timer dispatch not running
- maintenance drain mode enabled
- no workflow executor capacity

Checks:

- verify timer script `ignition/timer/tick` is active
- check `/admin` maintenance + executor diagnostics
- check queueName mismatch between start and dispatch

## Symptom: runs stuck in `PENDING`

Likely causes:

- claim happened but dispatch submit failed
- partition is busy and claims are getting released/retried
- executor pressure

Checks:

- inspect `/admin` executor diagnostics
- verify partition key contention patterns
- inspect logs around dispatch and claim release

## Symptom: too many cancellations/timeouts

Likely causes:

- global retention timeout too aggressive
- workflow timeout too short
- step code not checking cancel quickly enough

Checks:

- retention policy values in `/admin`
- per-run timeout passed to `start(...)`
- workflow loops using `checkCancelled()` + chunked waits

## Symptom: weird behavior after script save/deploy

Likely causes:

- mixed old/new interpreter state
- cutover not done with maintenance flow

Checks:

- use drain -> wait drained -> swap -> resume pattern
- confirm generation increments after swap

## DB-level checks (optional but useful)

Tables to inspect:

- `workflows.workflow_status`
- `workflows.operation_outputs`
- `workflows.workflow_events`
- `workflows.streams`
- `workflows.notifications`

Quick questions to answer with DB data:

- Is this run still moving (`updated_at_epoch_ms` changing)?
- Was it ever started (`started_at_epoch_ms`)?
- Did it write step outputs?
- Did error payload include cancellation/timeout reason?

## Safe incident workflow

1. Enter maintenance drain.
2. Watch active counts drop.
3. Decide if swap is needed.
4. Swap only when drained.
5. Exit maintenance.
6. Re-test one demo workflow before full traffic.

## Related docs

- [Concurrency + Lifecycle](../concepts/concurrency-lifecycle.md)
- [Admin API](../api-reference/api-admin.md)
- [Service API](../api-reference/api-service.md)
