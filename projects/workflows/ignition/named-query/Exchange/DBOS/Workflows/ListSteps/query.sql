SELECT
  function_id,
  function_name,
  started_at_epoch_ms,
  completed_at_epoch_ms,
  to_timestamp(started_at_epoch_ms / 1000.0)   AS started_ts,
  to_timestamp(completed_at_epoch_ms / 1000.0) AS completed_ts,
  output,
  error,
  child_workflow_id,
  CASE
    WHEN NULLIF(error, '') IS NOT NULL THEN 'ERROR'
    WHEN completed_at_epoch_ms IS NOT NULL THEN 'SUCCESS'
    WHEN started_at_epoch_ms IS NOT NULL THEN 'RUNNING'
    ELSE 'PENDING'
  END AS step_status
FROM dbos.operation_outputs
WHERE workflow_uuid = :uuid
ORDER BY function_id ASC;
