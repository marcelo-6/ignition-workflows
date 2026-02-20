---
icon: lucide/layout-dashboard
---

# Workflows Console UI Walkthrough

![Demo](../images/ui/demo.gif "Quick demo")

## Navigation map

- `/` -> Start workflow form
- `/runs` -> Runs monitor
- `/runs/:uuid` -> Run details + controls
- `/runs/:uuid/steps` -> Step timeline and output table
- `/queues` -> Queue/status overview
- `/admin` -> Maintenance + retention + diagnostics

## Start workflow (`/`)

Pick a workflow, fill demo values, and start a run.

## Monitor runs (`/runs`)

Filter by name/status/time range and open a specific run.

![Run monitor](../images/ui/02_runs_monitor_filters.png "Run monitor page showing filters and workflow list")

### Components

- Filter by hour, workflow name or status
 ![Filter](../images/ui/02b_runs_monitor_filters.png "Filter by hour, workflow name or status")

- Workflows status, id, name, queue, status, created/timeout datetime and duration
 ![Filter](../images/ui/02c_runs_monitor_filters.png "Workflows status, id, name, queue, status, created/timeout datetime and duration")

- Selected workflows information, id, name, queue, status, created/timeout datetime and duration

 ![Filter](../images/ui/02d_runs_monitor_filters.png "Selected workflows information, id, name, queue, status, created/timeout datetime and duration")

## Steps timeline (`/runs/:uuid/steps`)

Use this to see step order, timing, and output/error rows.

- Step tree/timeline and step output table linked to same run UUID

![Image](../images/ui/04_workflow_steps_timeline.png "Step tree/timeline and step output table linked to same run UUID")

## Admin controls (`/admin`)

Main admin view for maintenance operations and retention settings.

- Drain/Cancel/Swap/Resume controls

![Image](../images/ui/06_admin_maintenance_controls.png "Drain/Cancel/Swap/Resume controls")

- Concurrency overview status

![Image](../images/ui/06b_admin_maintenance_controls.png "Concurrency overview status")

- Retention policy tab

![Image](../images/ui/08_admin_retention_policy.png "Retention policy tab")
