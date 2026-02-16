SELECT "key", value_json, created_at_epoch_ms 
FROM workflows.streams
WHERE workflow_id = :workflowId::uuid
ORDER BY "offset" DESC
