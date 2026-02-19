-- Workflows durable-ish executor schema (Postgres)

CREATE SCHEMA IF NOT EXISTS workflows;

-- optional: a tiny metadata table
CREATE TABLE IF NOT EXISTS workflows.schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- TODO remove?
INSERT INTO workflows.schema_meta(key, value)
VALUES ('schema_version', '1')
ON CONFLICT (key) DO NOTHING;

-- Core workflow runs table
CREATE TABLE IF NOT EXISTS workflows.workflow_status (
  workflow_id UUID PRIMARY KEY,
  workflow_name TEXT NOT NULL,

  status TEXT NOT NULL,                         -- ENQUEUED|PENDING|RUNNING|SUCCESS|ERROR|CANCELLED
  queue_name TEXT NOT NULL DEFAULT 'default',
  partition_key TEXT NULL,
  priority INT NOT NULL DEFAULT 0,

  deduplication_id TEXT NULL,
  application_version TEXT NULL,

  inputs_json TEXT NULL,                        -- {resolved:{}, param_source:{...}}
  result_json TEXT NULL,
  error_json TEXT NULL,

  created_at_epoch_ms BIGINT NOT NULL,
  updated_at_epoch_ms BIGINT NOT NULL,
  started_at_epoch_ms BIGINT NULL,
  completed_at_epoch_ms BIGINT NULL,

  claimed_by TEXT NULL,
  claimed_at_epoch_ms BIGINT NULL,
  heartbeat_at_epoch_ms BIGINT NULL,

  deadline_epoch_ms BIGINT NULL                 -- per-run timeout (optional)
);

-- Claim index
CREATE INDEX IF NOT EXISTS idx_workflow_claim
ON workflows.workflow_status(queue_name, status, priority DESC, created_at_epoch_ms ASC);

-- Partition status index (helps dashboards)
CREATE INDEX IF NOT EXISTS idx_workflow_partition
ON workflows.workflow_status(queue_name, partition_key, status);

-- Dedup index (active-window only: block duplicates while ENQUEUED/PENDING)
DROP INDEX IF EXISTS workflows.uq_workflow_dedup;
CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_dedup
ON workflows.workflow_status(queue_name, deduplication_id)
WHERE deduplication_id IS NOT NULL
  AND status IN ('ENQUEUED', 'PENDING');

-- Step outputs (durable replay)
CREATE TABLE IF NOT EXISTS workflows.operation_outputs (
  output_id BIGSERIAL PRIMARY KEY,
  workflow_id UUID NOT NULL,
  call_seq INT NOT NULL,                         -- deterministic step invocation order
  step_name TEXT NOT NULL,

  status TEXT NOT NULL,                          -- SUCCESS|ERROR
  output_json TEXT NULL,
  error_json TEXT NULL,

  started_at_epoch_ms BIGINT NOT NULL,
  completed_at_epoch_ms BIGINT NULL,
  attempts INT NOT NULL DEFAULT 1,

  UNIQUE(workflow_id, call_seq)
);

CREATE INDEX IF NOT EXISTS idx_outputs_workflow
ON workflows.operation_outputs(workflow_id, call_seq);

-- Mailbox notifications
CREATE TABLE IF NOT EXISTS workflows.notifications (
  notification_id BIGSERIAL PRIMARY KEY,
  destination_uuid UUID NOT NULL,               -- workflow_id
  topic TEXT NOT NULL,
  message_json TEXT NOT NULL,
  message_uuid UUID NULL,
  created_at_epoch_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notifications_recv
ON workflows.notifications(destination_uuid, topic, created_at_epoch_ms ASC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_notifications_dedup
ON workflows.notifications(destination_uuid, topic, message_uuid)
WHERE message_uuid IS NOT NULL;

-- Latest events (cheap dashboard reads)
CREATE TABLE IF NOT EXISTS workflows.workflow_events (
  workflow_id UUID NOT NULL,
  key TEXT NOT NULL,
  value_json TEXT NULL,
  updated_at_epoch_ms BIGINT NOT NULL,
  PRIMARY KEY (workflow_id, key)
);

-- Event history (timeline)
CREATE TABLE IF NOT EXISTS workflows.workflow_events_history (
  event_id BIGSERIAL PRIMARY KEY,
  workflow_id UUID NOT NULL,
  key TEXT NOT NULL,
  value_json TEXT NULL,
  created_at_epoch_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_hist
ON workflows.workflow_events_history(workflow_id, key, created_at_epoch_ms);

-- Streams (append-only logs)
CREATE TABLE IF NOT EXISTS workflows.streams (
  stream_id BIGSERIAL PRIMARY KEY,
  workflow_id UUID NOT NULL,
  key TEXT NOT NULL,
  "offset" BIGINT NOT NULL,
  value_json TEXT NULL,
  created_at_epoch_ms BIGINT NOT NULL,
  UNIQUE(workflow_id, key, "offset")
);

CREATE INDEX IF NOT EXISTS idx_stream_tail
ON workflows.streams(workflow_id, key, "offset");

-- Stream heads (atomic "offset" increments)
CREATE TABLE IF NOT EXISTS workflows.stream_heads (
  workflow_id UUID NOT NULL,
  key TEXT NOT NULL,
  next_offset BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY(workflow_id, key)
);

-- Parameter Templates (append-only versions)
CREATE TABLE IF NOT EXISTS workflows.param_templates (
  workflow_name TEXT NOT NULL,
  template_name TEXT NOT NULL,
  template_version INT NOT NULL,
  schema_json TEXT NULL,
  status TEXT NOT NULL DEFAULT 'active',        -- active|obsolete
  description TEXT NULL,
  created_by TEXT NULL,
  created_at_epoch_ms BIGINT NOT NULL,
  PRIMARY KEY (workflow_name, template_name, template_version)
);

CREATE INDEX IF NOT EXISTS idx_templates_latest
ON workflows.param_templates(workflow_name, template_name, status, template_version DESC);

-- Retention policies (single row)
CREATE TABLE IF NOT EXISTS workflows.retention_config (
  id INT PRIMARY KEY,
  time_threshold_hours INT NULL,                -- NULL disables
  rows_threshold BIGINT NOT NULL DEFAULT 1000000,
  global_timeout_hours INT NULL,                -- NULL disables
  updated_at_epoch_ms BIGINT NOT NULL
);

INSERT INTO workflows.retention_config(id, time_threshold_hours, rows_threshold, global_timeout_hours, updated_at_epoch_ms)
VALUES (1, NULL, 1000000, NULL, (EXTRACT(EPOCH FROM NOW())*1000)::BIGINT)
ON CONFLICT (id) DO NOTHING;

-- Parameter Enums (append-only versions)
CREATE TABLE IF NOT EXISTS workflows.param_enums (
  enum_name TEXT NOT NULL,
  enum_version INT NOT NULL,
  values_json TEXT NOT NULL,                  -- [{"label","value","isDisabled"}...]
  status TEXT NOT NULL DEFAULT 'active',      -- active|obsolete
  description TEXT NULL,
  created_by TEXT NULL,
  created_at_epoch_ms BIGINT NOT NULL,
  PRIMARY KEY (enum_name, enum_version)
);

CREATE INDEX IF NOT EXISTS idx_enums_latest
ON workflows.param_enums(enum_name, status, enum_version DESC);
