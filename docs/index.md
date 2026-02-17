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

## Basic functionality that this library enables

- Concurrent execution with retry behavior and operator control (HOLD/RESUME/STOP).
- Workflow and and their steps outputs are stored in Postgres.
- Work can be queued from Ignition events (tag changes, button presses, etc).
- Architecture is set up to support automated testing and future external executors (CPython, Go, TypeScript, Java). External executors are just applications that use DBOS. (see current limitations for details)

## Use Cases

This library complements Ignition's functionality in a few lines of code. For example:

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

=== "Tag Event Changes"

    The Ignition tag system has a limited number of threads [available]. For that reason it is imperative to keep any tag event scripts execution to be fast (`<10ms`). If you have long running functions/tasks that need to be executed on a tag change event `Ignition Workflows` makes it easy to accomplish this
  
    [Forum discussion about this](https://forum.inductiveautomation.com/t/java-concurrent-queue-for-tag-change-scripts/82044/21)

    [Detailed Example](getting-started.md)

    [available]: https://forum.inductiveautomation.com/t/tag-change-missed-events/37764 "Here is one of many forum posts about it"
    
    
    ``` python title="Value Changed Tag Event Script"
    def valueChanged(tag, tagPath, previousValue, currentValue, initialChange, missedEvents):
      exchange.workflows.api.service.enqueueInMemory("ignition_workflow") # (1)!
    ```

     1. [Details](getting-started.md) on what happens after you call `enqueueInMemory`

    

=== "Perspective"

    [Detailed Example](getting-started/getting-started/#){ data-preview }

    ``` python title="onActionPerformed event script for a button in a view"

    def runAction(self, event):
        ret = exchange.workflows.api.service.enqueue("ignition_workflow") # (1)!
    ```

    1.  The workflow decorator lets use your existing functions as tasks to be executed later. You can queue them to be executed. See [Example](getting-started.md)

=== "Event streams"

    ``` python
    TODO add a full example
    ```

=== "Http requests"

    ``` python
    TODO add a full example
    ```

=== "All"

    ``` python
    TODO add a full example
    ```

## Limitations

This project is not trying to completly implement DBOS 1:1 and not trying to replace the SFC module.
