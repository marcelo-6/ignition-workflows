SELECT error
FROM dbos.operation_outputs
WHERE workflow_uuid=:uuid AND function_id=1;

