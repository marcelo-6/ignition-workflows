SELECT
  COALESCE(queue_name, '(none)') AS queue_name,
  COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
  COUNT(*) FILTER (WHERE status = 'RUNNING') AS running,
  COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success,
  COUNT(*) FILTER (WHERE status = 'ERROR')   AS error,
  MIN(created_at) FILTER (WHERE status = 'PENDING') AS oldest_pending_created_at,
  MAX(updated_at) AS latest_update_at
FROM dbos.workflow_status
WHERE
  ( :fromEpochMs IS NULL OR created_at >= :fromEpochMs )
  AND ( :toEpochMs IS NULL OR created_at <  :toEpochMs )
GROUP BY COALESCE(queue_name, '(none)')
ORDER BY pending DESC, running DESC, error DESC;
