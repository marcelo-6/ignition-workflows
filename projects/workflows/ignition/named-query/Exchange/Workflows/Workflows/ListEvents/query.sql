SELECT key, value_json, updated_at_epoch_ms
FROM workflows.workflow_events
WHERE workflow_id = :workflowId::uuid
ORDER BY key ASC;
