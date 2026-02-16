UPDATE workflows.retention_config
SET time_threshold_hours = :timeThresholdHours,
    rows_threshold = :rowsThreshold,
    global_timeout_hours = :globalTimeoutHours,
    updated_at_epoch_ms = :updatedAtEpochMs
WHERE id = 1;
