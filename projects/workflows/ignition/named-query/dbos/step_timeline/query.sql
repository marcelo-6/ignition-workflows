SELECT function_id, function_name, started_at_epoch_ms, completed_at_epoch_ms, error
FROM dbos.operation_outputs
WHERE workflow_uuid = :uuid
ORDER BY function_id ASC;
