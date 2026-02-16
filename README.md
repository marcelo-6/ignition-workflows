# Ignition Workflows

This is a workflow orchestration framework for long-running tasks in Ignition, as a more maintainable alternative to SFC (but with a more limited functionality).

Goal:
- I've used SFCs in other projects in the past and it can be hard to manage the code and troubleshoot. I read about DBOS and found it gave a good framework for async long running tasks. I thought that porting over some of their design into Ignition (without using SFC module or creating thirdparty module) would be a fun personal project to get me a little more familiar with async multi-threaded Ignition Scripting.

What is currently implemented:
- Durable concurrent execution with retry behavior and operator control (HOLD/RESUME/STOP).
- Workflow and and their steps outputs are stored in Postgres.
- Work can be queued from Ignition events (tag changes, button presses, etc).
- Architecture is set up to support automated testing and future external executors (CPython, Go, TypeScript, Java). External executors are just applications that use DBOS.

Current items in my implementation vs DBOS that I am missing (and in order of my next priorities):
1. Async step execution (main TODO for next release).
2. Scheduled workflow (you can for now use a gateway scheduled script to accomplish the same).
3. Debounce workflows.
4. Child workflow orchestration (workflow starting another workflow, this is possible now but they have no connection to eachother, in DBOS they do).


## Status
Early public release.