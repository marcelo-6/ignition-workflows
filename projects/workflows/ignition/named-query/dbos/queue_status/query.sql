SELECT
  ws.queue_name,
  ws.status,
  COUNT(*) AS count,
  MIN(ws.created_at) AS oldest_created_at_ms,
  MAX(ws.updated_at) AS newest_updated_at_ms,
  SUM(CASE WHEN ws.status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
FROM dbos.workflow_status ws
WHERE
  ( :queue_name IS NULL OR :queue_name = '' OR ws.queue_name = :queue_name )
  AND ws.created_at >= (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - (:lookback_minutes * 60 * 1000)
GROUP BY ws.queue_name, ws.status
ORDER BY ws.queue_name ASC, ws.status ASC;

