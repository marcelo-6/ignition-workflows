# exchange/workflows/tests/suites/test_db.py
"""DB contract tests that validate claim/start/release/terminal-guard semantics."""

import json

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.db import uuid4, nowMs
from exchange.workflows.tests.support.base_case import WorkflowTestCase


class TestDbContractSuite(WorkflowTestCase):
    """Raw DB adapter checks for status transitions and claim field handling."""

    SUITE_NAME = "db_contract"

    def test_claim_does_not_set_started(self):
        """claimEnqueued should move row to PENDING without filling started/deadline."""
        rt = getWorkflows(dbName=self.dbName)
        wid = uuid4()
        created = long(nowMs())
        self.db.insertWorkflow(
            workflowId=wid,
            workflowName="tests.db_contract_claim",
            queueName=settings.QUEUE_DEFAULT,
            partitionKey=None,
            priority=0,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )
        tx = self.db.begin()
        try:
            claimed = self.db.claimEnqueued(
                settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx
            )
            self.db.commit(tx)
        finally:
            self.db.close(tx)
        self.assertEqual(len(claimed), 1)
        row = self.db.getWorkflow(wid)
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("status")), settings.STATUS_PENDING)
        self.assertIsNone(row.get("started_at_epoch_ms"))
        self.assertIsNone(row.get("deadline_epoch_ms"))
        self.db.releaseClaim(wid)
        self.db.cancelWorkflow(wid, "test_cleanup")

    def test_mark_running_sets_started_and_deadline(self):
        """markRunningIfClaimed should atomically set RUNNING, started_at, and deadline."""
        rt = getWorkflows(dbName=self.dbName)
        wid = uuid4()
        created = long(nowMs())
        self.db.insertWorkflow(
            workflowId=wid,
            workflowName="tests.db_contract_mark_running",
            queueName=settings.QUEUE_DEFAULT,
            partitionKey=None,
            priority=0,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )
        tx = self.db.begin()
        try:
            self.db.claimEnqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
            self.db.commit(tx)
        finally:
            self.db.close(tx)

        startedMs = long(nowMs())
        n = self.db.markRunningIfClaimed(
            workflowId=wid,
            executorId=rt.executor_id,
            startedMs=startedMs,
            timeoutMs=2000,
        )
        self.assertEqual(int(n), 1)
        row = self.db.getWorkflow(wid)
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("status")), settings.STATUS_RUNNING)
        self.assertEqual(long(row.get("started_at_epoch_ms")), startedMs)
        delta = long(row.get("deadline_epoch_ms") - row.get("started_at_epoch_ms"))
        self.assertEqual(delta, 2000)
        self.db.cancelWorkflow(wid, "test_cleanup")

    def test_release_claim_clears_claim_fields(self):
        """releaseClaim should clear claim metadata and move row back to ENQUEUED."""
        rt = getWorkflows(dbName=self.dbName)
        wid = uuid4()
        created = long(nowMs())
        self.db.insertWorkflow(
            workflowId=wid,
            workflowName="tests.db_contract_releaseClaim",
            queueName=settings.QUEUE_DEFAULT,
            partitionKey=None,
            priority=0,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )
        tx = self.db.begin()
        try:
            self.db.claimEnqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
            self.db.commit(tx)
        finally:
            self.db.close(tx)

        n = self.db.releaseClaim(wid)
        self.assertEqual(int(n), 1)
        row = self.db.getWorkflow(wid)
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("status")), settings.STATUS_ENQUEUED)
        self.assertIsNone(row.get("claimed_by"))
        self.assertIsNone(row.get("claimed_at_epoch_ms"))
        self.assertIsNone(row.get("started_at_epoch_ms"))
        self.assertIsNone(row.get("deadline_epoch_ms"))
        self.db.cancelWorkflow(wid, "test_cleanup")

    def test_terminal_guard_blocks_overwrite(self):
        """updateWorkflowStatusIfNotTerminal should not overwrite already terminal rows."""
        wid = uuid4()
        created = long(nowMs())
        self.db.insertWorkflow(
            workflowId=wid,
            workflowName="tests.db_contract_terminal_guard",
            queueName=settings.QUEUE_DEFAULT,
            partitionKey=None,
            priority=0,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )
        self.db.cancelWorkflow(wid, "test_terminal_guard")
        n = self.db.updateWorkflowStatusIfNotTerminal(
            workflowId=wid,
            status=settings.STATUS_SUCCESS,
            fields={"result_json": json.dumps({"bad": True})},
        )
        self.assertEqual(int(n), 0)
        row = self.db.getWorkflow(wid)
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("status")), settings.STATUS_CANCELLED)
