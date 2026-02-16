SELECT 
    queue_name,
    status,
    COUNT(*) AS count
FROM workflows.workflow_status
WHERE (:queueName IS NULL OR :queueName = '' OR queue_name = :queueName)
GROUP BY queue_name, status
ORDER BY queue_name, status;