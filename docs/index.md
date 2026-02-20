---
icon: lucide/book-open
---

# Ignition Workflows

A simple workflow orchestration library for [Ignition](http://ia.io).

??? info "Why?"

    I've used SFCs in other projects in the past and it can be hard to manage the code and troubleshoot. 
    
    I read about [DBOS](https://docs.dbos.dev/) and found it gave a good framework for async long running tasks. I thought that porting over some of their design into Ignition (without using SFC module or creating a third-party module) would be a fun personal project to get me a little more familiar with async multi-threaded Ignition scripting. 
    
    Ignition is event driven and there are [some things](https://forum.inductiveautomation.com/t/managing-multiple-asynchronous-threads/37185) to keep in mind when trying to create/manage finite state machines and this library tries to handle some of that for you. 

Think of workflows as tasks that can have subtasks. This library just gives you a structured way to start/queue those tasks and gives you some reliability (retry on failure etc) and also creates threads as needed and attemps to manage the lifecycle of those tasks/threads.

## What you can do with it

- Queue work from Perspective, gateway scripts, or tag events.
- Keep status, outputs, and events in Postgres.
- Run multiple workflows concurrently and let the library manage concurrency and retry behavior

## Quick way in

- [Getting Started](getting-started/getting-started.md).
- [Examples / Operations](examples/operations.md).
- [UI Walkthrough](ui/workflows-console.md).
- [Architecture](concepts/architecture.md) and [Concurrency](concepts/concurrency-lifecycle.md).

## Quick snippets

=== "Simple Decorated Function"

    [Detailed Example](getting-started.md)
    ``` python
    from exchange.workflows.engine.runtime import workflow
    from exchange.workflows.engine.runtime import step
    from exchange.workflows.engine.instance import getWorkflows

    @step() # (2)!
    def step_one():
        print("Step one completed!")

    @step()
    def step_two():
        print("Step two completed!")

    @workflow() # (1)!
    def ignition_workflow():
        step_one()
        step_two()
    ```

    1.  The workflow decorator lets use your existing functions as tasks to be executed later. You can queue them to be executed. See [Example](getting-started.md)

    2.  The step decorator lets use your existing functions as steps/subtasks. They have retry on failure capabilites. See [Example](getting-started.md)

=== "From a Perspective button"

    [Detailed Example](getting-started/getting-started/#){ data-preview }
    TODO update the link

    ``` python title="onActionPerformed event script for a button in a view"

    def runAction(self, event):
        ret = exchange.workflows.api.service.enqueue("ignition_workflow") # (1)!
    ```

    1.  The workflow decorator lets use your existing functions as tasks to be executed later. You can queue them to be executed. See [Example](getting-started.md)

=== "From Tag Change Events"

    The Ignition tag system has a limited number of threads [available]. For that reason it is imperative to keep any tag event scripts execution to be fast (`<10ms`). If you have long running functions/tasks that need to be executed on a tag change event `Ignition Workflows` makes it easy to accomplish this
  
    [Forum discussion about this](https://forum.inductiveautomation.com/t/java-concurrent-queue-for-tag-change-scripts/82044/21)

    [Detailed Example](getting-started.md) 
    TODO update the link

    [available]: https://forum.inductiveautomation.com/t/tag-change-missed-events/37764 "Here is one of many forum posts about it"
    
    
    ``` python title="Value Changed Tag Event Script"
    def valueChanged(tag, tagPath, previousValue, currentValue, initialChange, missedEvents):
      exchange.workflows.api.service.enqueueInMemory("ignition_workflow") # (1)!
    ```

     1. [Details](getting-started.md) on what happens after you call `enqueueInMemory`

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
