SELECT
  ws.created_at,
  ws.workflow_uuid,
  ws.name,
  ws.status,
  COALESCE(op.step_count, 0) AS step_count
FROM dbos.workflow_status ws
LEFT JOIN (
  SELECT workflow_uuid, COUNT(*)::INT AS step_count
  FROM dbos.operation_outputs
  GROUP BY workflow_uuid
) op ON op.workflow_uuid = ws.workflow_uuid
WHERE
  -- If from/to are NULL, don't constrain by time
  ( :fromEpochMs = 0 OR ws.created_at >= :fromEpochMs )
  AND ( :toEpochMs = 0 OR ws.created_at <  :toEpochMs )

  -- If filter param is NULL or empty string => ignore filter
  AND ( NULLIF(TRIM(:name), '') IS NULL OR ws.name ILIKE '%' || TRIM(:name) || '%' )
  AND ( NULLIF(TRIM(:uuid),   '') IS NULL OR ws.workflow_uuid ILIKE '%' || TRIM(:uuid) || '%' )
  AND ( NULLIF(TRIM(:version), '') IS NULL OR ws.application_version ILIKE '%' || TRIM(:version) || '%' )

  -- Status exact match, ignored if NULL/empty
  AND ( NULLIF(TRIM(:status), '') IS NULL OR ws.status = TRIM(:status) )
ORDER BY ws.created_at DESC
LIMIT COALESCE(:limit, 200)
OFFSET COALESCE(:offset, 0);
