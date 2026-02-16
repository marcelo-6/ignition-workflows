---
icon: lucide/map
---

# Roadmap

Current status: early public release, with core durable execution and operator control already implemented.

## Implemented

- Durable-ish workflow execution with Postgres persistence.
- Cooperative command handling (`HOLD`, `RESUME`, `STOP`, `RESET`).
- Step replay cache via `operation_outputs`.
- Events/streams for monitoring.
- Runtime maintenance mode and generation swap.
- Template + enum parameter model for operator forms.

## Next up (high priority)

1. True async step execution.
2. Scheduled workflow.
3. Debounce workflows.
4. Child-workflow orchestration relationships.

## Planned improvements

- Stronger programmatic APIs for parameter sets.
- More packaged examples around production process patterns.
- Optional external executors and expanded interop patterns.

## Explicit non-goals (current)

- Multi-version workflow registry with arbitrary old-code replay.
- Hard thread preemption of running steps.
- Full DB migration framework in core runtime.
- Heavy control-plane orchestration service.

