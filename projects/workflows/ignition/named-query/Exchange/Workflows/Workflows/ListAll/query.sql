SELECT
    workflow_id,
    LEFT(workflow_id::text, 4) || '...' || RIGHT(workflow_id::text, 4) AS short_id,
    workflow_name,
    status,
    queue_name,
    partition_key,
    priority,
    created_at_epoch_ms,
    started_at_epoch_ms,
    updated_at_epoch_ms,
    deadline_epoch_ms,
    completed_at_epoch_ms,
CASE
    WHEN completed_at_epoch_ms IS NULL OR started_at_epoch_ms IS NULL THEN NULL

    -- < 1 second → show ms
    WHEN (completed_at_epoch_ms - started_at_epoch_ms) < 1000 THEN
        (completed_at_epoch_ms - started_at_epoch_ms) || ' ms'

    -- < 1 minute → show seconds with milliseconds
    WHEN (completed_at_epoch_ms - started_at_epoch_ms) < 60000 THEN
        ROUND((completed_at_epoch_ms - started_at_epoch_ms) / 1000.0, 3) || ' s'

    -- < 1 hour → show minutes + seconds
    WHEN (completed_at_epoch_ms - started_at_epoch_ms) < 3600000 THEN
        ((completed_at_epoch_ms - started_at_epoch_ms) / 60000) || 'm ' ||
        (((completed_at_epoch_ms - started_at_epoch_ms) % 60000) / 1000) || 's'

    -- < 1 day → show hours + minutes + seconds
    WHEN (completed_at_epoch_ms - started_at_epoch_ms) < 86400000 THEN
        ((completed_at_epoch_ms - started_at_epoch_ms) / 3600000) || 'h ' ||
        (((completed_at_epoch_ms - started_at_epoch_ms) % 3600000) / 60000) || 'm ' ||
        (((completed_at_epoch_ms - started_at_epoch_ms) % 60000) / 1000) || 's'

    -- ≥ 1 day → show days + hours + minutes
    ELSE
        ((completed_at_epoch_ms - started_at_epoch_ms) / 86400000) || 'd ' ||
        (((completed_at_epoch_ms - started_at_epoch_ms) % 86400000) / 3600000) || 'h ' ||
        (((completed_at_epoch_ms - started_at_epoch_ms) % 3600000) / 60000) || 'm'
END AS run_duration


FROM workflows.workflow_status
WHERE (
        :queueName IS NULL
        OR :queueName = ''
        OR queue_name = :queueName
      )
  AND (
        :hours = 0
        OR created_at_epoch_ms >= (
              EXTRACT(EPOCH FROM NOW()) * 1000
              - (:hours * 60 * 60 * 1000)
          )
      )
    AND (
        :workflowName IS NULL
        OR :workflowName = ''
        OR workflow_name LIKE '%' || :workflowName || '%'
      )
  AND (
        :status IS NULL
        OR :status = ''
        OR status = :status
      )
ORDER BY priority DESC, created_at_epoch_ms DESC
LIMIT :limit;