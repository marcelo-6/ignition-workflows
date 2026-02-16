TRUNCATE TABLE
  workflows.workflow_status,
  workflows.operation_outputs,
  workflows.notifications,
  workflows.workflow_events,
  workflows.workflow_events_history,
  workflows.streams,
  workflows.stream_heads,
  workflows.param_templates,
  workflows.param_sets,
  workflows.retention_config,
  workflows.schema_meta
RESTART IDENTITY CASCADE;