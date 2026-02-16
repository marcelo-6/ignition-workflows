SELECT workflow_uuid, name, status, queue_name, queue_partition_key, created_at, updated_at
FROM dbos.workflow_status
ORDER BY created_at DESC;
