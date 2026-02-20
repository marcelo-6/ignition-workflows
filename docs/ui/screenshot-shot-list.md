---
icon: lucide/camera
---

# Screenshot Shot List

These are the screenshots we want in docs. For now we keep tracked placeholders so links don’t break.

## Standards

- Use demo-only values (no real customer names, ids, IPs, tokens, creds).
- Keep one viewport style per batch.
- Save files to `docs/.images/ui/`.
- Replace placeholder files without changing filenames.
- Current tracked placeholders are `.svg` so docs render now.

## Priority list

| Priority | Filename | Route | What should be visible | Demo data |
| --- | --- | --- | --- | --- |
| P0 | `01_start_workflow_form.svg` | `/` | Workflow/template selection + start button | `BATCH-001`, `AUTO` |
| P0 | `02_runs_monitor_filters.svg` | `/runs` | Filtered list with mixed statuses | 3-5 demo runs |
| P0 | `03_workflow_details_commands.svg` | `/runs/:uuid` | Status badge + command buttons | one active `demo.commands_60s` run |
| P0 | `04_workflow_steps_timeline.svg` | `/runs/:uuid/steps` | Step tree/timeline + output table | hold/resume cycle run |
| P0 | `05_queue_status_counts.svg` | `/queues` | Queue counts by status | default queue mixed status |
| P0 | `06_admin_maintenance_controls.svg` | `/admin` | Drain/Cancel/Swap/Resume controls | maintenance disabled initially |
| P1 | `07_maintenance_enabled_state.svg` | `/runs` + `/admin` | Maintenance banner/state visible | reason `docs-demo-drain` |
| P1 | `08_admin_retention_policy.svg` | `/admin` tab 2 | Time/rows/global timeout fields | `24`, `100000`, `2` |
| P1 | `09_workflow_json_events.svg` | `/runs/:uuid` | Input/output/error/events payloads | run with non-empty logs |
| P1 | `10_workflow_stream_table.svg` | popup from details | Stream rows with offsets | progress log entries |
| P2 | `11_admin_logs.svg` | `/admin/logs` | Logs pane/iframe loaded | workflow logger lines |
| P2 | `12_admin_threads.svg` | `/admin/threads` | Thread diagnostics page | workflow/step pools visible |

## Quick flow for the human taking screenshots

1. Seed demo data.
2. Start one long-running `demo.commands_60s` run.
3. Capture P0 screenshots in order.
4. Trigger drain mode and capture maintenance screenshots.
5. Replace placeholders in `docs/.images/ui/` with real images.
