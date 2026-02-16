-- Create schema
CREATE SCHEMA IF NOT EXISTS dbos;

-- Main workflow status table
CREATE TABLE IF NOT EXISTS dbos.workflow_status (
  workflow_uuid TEXT PRIMARY KEY,
  status TEXT,
  name TEXT,
  authenticated_user TEXT,
  assumed_role TEXT,
  authenticated_roles TEXT,
  output TEXT,
  error TEXT,
  executor_id TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  application_version TEXT,
  application_id TEXT,
  class_name VARCHAR(255),
  config_name VARCHAR(255),
  recovery_attempts BIGINT,
  queue_name TEXT,
  workflow_timeout_ms BIGINT,
  workflow_deadline_epoch_ms BIGINT,
  started_at_epoch_ms BIGINT,
  deduplication_id TEXT,
  inputs TEXT,
  priority INTEGER NOT NULL,
  queue_partition_key TEXT,
  forked_from TEXT,
  owner_xid TEXT,
  CONSTRAINT uq_workflow_status_queue_name_dedup_id
    UNIQUE (queue_name, deduplication_id)
);

-- Indexes for workflow_status
CREATE INDEX IF NOT EXISTS workflow_status_created_at_index
  ON dbos.workflow_status(created_at);

CREATE INDEX IF NOT EXISTS workflow_status_executor_id_index
  ON dbos.workflow_status(executor_id);

CREATE INDEX IF NOT EXISTS workflow_status_status_index
  ON dbos.workflow_status(status);

-- Operation outputs
CREATE TABLE IF NOT EXISTS dbos.operation_outputs (
  workflow_uuid TEXT NOT NULL
    REFERENCES dbos.workflow_status(workflow_uuid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  function_id INTEGER NOT NULL,
  function_name TEXT NOT NULL,
  output TEXT,
  error TEXT,
  child_workflow_id TEXT,
  started_at_epoch_ms BIGINT,
  completed_at_epoch_ms BIGINT,
  PRIMARY KEY (workflow_uuid, function_id)
);

-- Notifications
CREATE TABLE IF NOT EXISTS dbos.notifications (
  destination_uuid TEXT NOT NULL
    REFERENCES dbos.workflow_status(workflow_uuid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  topic TEXT,
  message TEXT NOT NULL,
  created_at_epoch_ms BIGINT NOT NULL,
  message_uuid TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_topic
  ON dbos.notifications(destination_uuid, topic);

-- Workflow events
CREATE TABLE IF NOT EXISTS dbos.workflow_events (
  workflow_uuid TEXT NOT NULL
    REFERENCES dbos.workflow_status(workflow_uuid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (workflow_uuid, key)
);

-- Workflow events history
CREATE TABLE IF NOT EXISTS dbos.workflow_events_history (
  workflow_uuid TEXT NOT NULL
    REFERENCES dbos.workflow_status(workflow_uuid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  function_id INTEGER NOT NULL,
  PRIMARY KEY (workflow_uuid, key, function_id)
);

-- Streams
CREATE TABLE IF NOT EXISTS dbos.streams (
  workflow_uuid TEXT NOT NULL
    REFERENCES dbos.workflow_status(workflow_uuid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  "offset" INTEGER NOT NULL,
  function_id INTEGER NOT NULL,
  PRIMARY KEY (workflow_uuid, key, "offset")
);