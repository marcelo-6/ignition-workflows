---
icon: lucide/book-marked
---

# Glossary

Short definitions used across docs and UI.

## Runtime

The workflow engine facade handling enqueue, dispatch, execution, and observability operations.

## Kernel

The persistent Java-side shared state used by runtime instances (executors, counters, queue objects, maintenance state).

## Dispatch

One timer-driven cycle that flushes in-memory queue items, claims DB work, and submits runnable jobs.

## Claim

When an `ENQUEUED` row is atomically marked `PENDING` and owned by one executor.

## Queue

Logical lane used for routing and claiming workflows (`queueName`).

## Partition key

Resource arbitration key (`partitionKey`) used to avoid concurrent work on the same unit/resource.

## Workflow

Orchestration function that coordinates step calls and control flow.

## Step

Side-effect unit of work with retry/durability behavior.

## In-memory enqueue

Fast enqueue path that avoids immediate DB write and flushes on next dispatch cycle.

## Maintenance mode

Operational mode used during cutovers/incidents.

- `drain`: no new claims, let active work finish
- `cancel`: drain + cooperative cancellation of queued/running work

## Generation

Counter incremented after successful runtime swap; helps identify active executor generation.

## Cooperative cancellation

Cancellation model where workflow/step code checks for cancellation and exits safely; no hard preemption.

## Events

Latest key/value state snapshots for a run (good for dashboards).

## Streams

Append-only log-style records for a run (good for timelines and troubleshooting context).
