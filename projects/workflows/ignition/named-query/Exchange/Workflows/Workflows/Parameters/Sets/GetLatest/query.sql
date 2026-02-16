SELECT DISTINCT ON (workflow_name, template_name, set_name)
  workflow_name, template_name, set_name, set_version, status, description, tags_json, created_at_epoch_ms
FROM workflows.param_sets
WHERE workflow_name = :workflowName
  AND template_name = :templateName
  AND status = 'active'
ORDER BY workflow_name, template_name, set_name, set_version DESC;
