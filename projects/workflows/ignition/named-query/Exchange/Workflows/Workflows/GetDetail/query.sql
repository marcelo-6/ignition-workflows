SELECT *
FROM workflows.workflow_status
WHERE workflow_id = :workflowId::uuid;
