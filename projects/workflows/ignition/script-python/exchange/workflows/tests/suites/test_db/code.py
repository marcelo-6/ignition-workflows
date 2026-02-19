# exchange/workflows/tests/suites/test_db.py
"""DB contract tests that validate claim/start/release/terminal-guard semantics."""

import json
import java.lang.Exception as JException
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

    def test_priority_ordering_contract(self):
        """Lower numeric priority should dequeue first (DBOS-compatible ascending order)."""
        rt = getWorkflows(dbName=self.dbName)
        queueName = "tests.queue.priority.%s" % long(nowMs())
        created = long(nowMs())
        lowPriorityWid = uuid4()
        highPriorityWid = uuid4()

        self.db.insertWorkflow(
            workflowId=lowPriorityWid,
            workflowName="tests.db_contract_priority_low",
            queueName=queueName,
            partitionKey=None,
            priority=10,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )
        self.db.insertWorkflow(
            workflowId=highPriorityWid,
            workflowName="tests.db_contract_priority_high",
            queueName=queueName,
            partitionKey=None,
            priority=1,
            deduplicationId=None,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created + 1,
            deadlineMs=None,
        )

        tx = self.db.begin()
        try:
            claimed = self.db.claimEnqueued(queueName, 2, rt.executor_id, tx=tx)
            self.db.commit(tx)
        finally:
            self.db.close(tx)

        claimedIds = [str(row.get("workflow_id")) for row in claimed]
        self.assertEqual(claimedIds, [highPriorityWid, lowPriorityWid])

        self.db.cancelWorkflow(lowPriorityWid, "test_cleanup")
        self.db.cancelWorkflow(highPriorityWid, "test_cleanup")

    def test_dedup_active_window(self):
        """Dedupe should block while ENQUEUED/PENDING and allow reuse after terminal."""
        queueName = "tests.queue.dedup.%s" % long(nowMs())
        dedupeId = "tests.dedup.active_window.%s" % long(nowMs())
        created = long(nowMs())
        firstWid = uuid4()
        secondWid = uuid4()

        self.db.insertWorkflow(
            workflowId=firstWid,
            workflowName="tests.db_contract_dedup_first",
            queueName=queueName,
            partitionKey=None,
            priority=0,
            deduplicationId=dedupeId,
            applicationVersion=None,
            inputsObj={"resolved": {}},
            createdMs=created,
            deadlineMs=None,
        )

        duplicateRejected = False
        try:
            self.db.insertWorkflow(
                workflowId=secondWid,
                workflowName="tests.db_contract_dedup_second_active",
                queueName=queueName,
                partitionKey=None,
                priority=0,
                deduplicationId=dedupeId,
                applicationVersion=None,
                inputsObj={"resolved": {}},
                createdMs=created + 1,
                deadlineMs=None,
            )
        except Exception:
            # TODO, this is not ideal what if the exception has nothing to do with the test? for now I will keep it
            duplicateRejected = True
        except JException:
            # TODO, this is not ideal what if the exception has nothing to do with the test? for now I will keep it
            duplicateRejected = True
        self.assertTrue(
            duplicateRejected,
            "expected duplicate dedupe insert to be rejected while first workflow is active",
        )

        n = self.db.updateWorkflowStatus(
            workflowId=firstWid,
            status=settings.STATUS_SUCCESS,
            fields={"completed_at_epoch_ms": long(nowMs())},
        )
        self.assertEqual(int(n), 1)

        try:
            self.db.insertWorkflow(
                workflowId=secondWid,
                workflowName="tests.db_contract_dedup_second_terminal",
                queueName=queueName,
                partitionKey=None,
                priority=0,
                deduplicationId=dedupeId,
                applicationVersion=None,
                inputsObj={"resolved": {}},
                createdMs=created + 2,
                deadlineMs=None,
            )
        except Exception as e:
            self.fail(
                "active-window dedupe expected allow-after-terminal behavior; "
                "check uq_workflow_dedup predicate for status ENQUEUED/PENDING: %s" % e
            )

        row = self.db.getWorkflow(secondWid)
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("status")), settings.STATUS_ENQUEUED)
        self.db.cancelWorkflow(secondWid, "test_cleanup")
