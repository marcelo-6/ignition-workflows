-- Clear all workflow data in the correct order

-- 1. Clear parent table (cascades to all children)
DELETE FROM dbos.workflow_status;

-- 2. Clear tables that do NOT cascade automatically
-- (These have no FK back to workflow_status)
DELETE FROM dbos.workflow_events;
DELETE FROM dbos.workflow_events_history;
DELETE FROM dbos.streams;
DELETE FROM dbos.operation_outputs;
DELETE FROM dbos.notifications;