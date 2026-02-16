SELECT
  workflow_id,
  workflow_name,
  status,
  started_at_epoch_ms,
  completed_at_epoch_ms
FROM workflows.workflow_status
WHERE workflow_id = :workflowId::uuid;
