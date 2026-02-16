SELECT call_seq, step_name, status, output_json, error_json,
       started_at_epoch_ms, completed_at_epoch_ms, attempts
FROM workflows.operation_outputs
WHERE workflow_id = :workflowId::uuid
ORDER BY call_seq ASC;
