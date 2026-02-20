---
icon: lucide/map
---

# Roadmap

* [x] Workflow execution with Postgres persistence.
* [x] Workflow requests queued by Postgres.
* [x] Step execution within workflow (sync) with Postgres persistence.
* [x] Step retry on failure
* [x] Procedure to making code changes in production to avoid [intepreter issues](concepts/architecture.md#ignitionjython-interpreter-reload-issue){ data-preview }.
* [x] Template + enum parameter for operator forms (recipe parameters for a given workflow).
* [x] Refactor `exchange.workflows.db` to conform with Exchange code standard
* [ ] True async step execution (workflow execution is already async)
* [ ] Child-workflow orchestration relationships
* [ ] Debounce workflows
* [ ] Scheduled workflow
* [ ] Confirm external executors are functional
* [ ] Publish to Ignition Exchange

## Documentation

* [x] Zensical project documentation
* [ ] More examples of usage
* [ ] More detailed API usage
* [ ] Add perspective pages screenshots

## Code Examples

* [x] Command handling (`HOLD`, `RESUME`, `STOP`, `RESET`).
* [ ] How to use templates
  
