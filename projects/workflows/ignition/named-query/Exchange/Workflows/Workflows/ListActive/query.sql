SELECT workflow_id, workflow_name, status, queue_name, partition_key, priority,
       created_at_epoch_ms, started_at_epoch_ms, updated_at_epoch_ms
FROM workflows.workflow_status
WHERE queue_name = :queueName
  AND status IN ('ENQUEUED','PENDING','RUNNING')
ORDER BY priority DESC, created_at_epoch_ms ASC
LIMIT :limit;
