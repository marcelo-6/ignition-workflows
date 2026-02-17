---
icon: lucide/wrench
---

# Steps and Workflow Authoring

## Core rules

- Keep orchestration logic in workflows.
- Keep side effects in steps.
- Keep steps small and retry-safe.
- Check cancel/command points frequently in long loops.

## Decorators

```python
from exchange.workflows.bootstrap import workflow, step
from exchange.workflows.engine.instance import getWorkflows
```

`bootstrap` re-exports the public decorators and runtime accessor.

## Minimal workflow

```python
from exchange.workflows.bootstrap import workflow
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.steps.sleep import sleep_chunked

@workflow(name="demo.small")
def demo_small(duration_s=10):
    rt = getWorkflows()
    loops = int(duration_s * 2)

    for i in range(loops):
        rt.checkCancelled()
        rt.progress("loop %s/%s" % (i + 1, loops))
        sleep_chunked(total_ms=500, chunk_ms=250)

    return {"ok": True}
```

## Built-in step library

### Tags

- `tags.read(path)`
- `tags.write(path, value)`
- `tags.write_confirm(path, value, confirm_path=None, timeout_ms=10000, poll_ms=250)`

### HTTP

- `http.request(method, url, headers=None, params=None, data=None, json_body=None, timeout_ms=10000)`

### DB (application DB, not engine internals)

- `db.query(dbName, sql, args=None)`
- `db.update(dbName, sql, args=None)`

### Wait/sleep

- `wait.until_tag_equals(path, expected, timeout_ms=10000, poll_ms=250)`
- `sleep.sleep_chunked(total_ms, chunk_ms=250)`

## Step retry options (DBOS-style)

```python
@step(
    name="my.step",
    options={
        "retries_allowed": True,
        "max_attempts": 3,
        "interval_seconds": 1.0,
        "backoff_rate": 2.0,
    },
)
def my_step(x):
    return x
```

## Mailbox pattern

Use mailbox commands for operator control.

```python
cmd = rt.recvCommand(currentState="RUNNING", topic="cmd", timeoutSeconds=0.2, maxMessages=8)
if cmd and cmd.get("cmd") == "HOLD":
    ...
```

## Registration reminder

Decorators register at import-time. Workflows must be imported by `exchange.workflows.apps.load()`.

Current project registration happens in:

- `exchange.workflows.apps.__init__.load`

If you add new workflow modules, import them there.
