SELECT DISTINCT ON (workflow_name, template_name)
  workflow_name, template_name, template_version, status, description, created_at_epoch_ms
FROM workflows.param_templates
WHERE workflow_name = :workflowName
  AND status = 'active'
ORDER BY workflow_name, template_name, template_version DESC;
