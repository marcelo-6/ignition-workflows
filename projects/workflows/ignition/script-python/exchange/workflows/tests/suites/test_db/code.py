# exchange/workflows/tests/suites/test_db.py
"""DB contract tests that validate claim/start/release/terminal-guard semantics."""

import json

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.db import uuid4, now_ms
from exchange.workflows.tests.support.base_case import WorkflowTestCase


class TestDbContractSuite(WorkflowTestCase):
	"""Raw DB adapter checks for status transitions and claim field handling."""

	SUITE_NAME = "db_contract"

	def test_claim_does_not_set_started(self):
		"""claim_enqueued should move row to PENDING without filling started/deadline."""
		rt = getWorkflows(dbName=self.dbName)
		wid = uuid4()
		created = long(now_ms())
		self.db.insert_workflow(
			workflow_id=wid,
			workflow_name="tests.db_contract_claim",
			queue_name=settings.QUEUE_DEFAULT,
			partition_key=None,
			priority=0,
			deduplication_id=None,
			application_version=None,
			inputs_obj={"resolved": {}},
			created_ms=created,
			deadline_ms=None,
		)
		tx = self.db.begin()
		try:
			claimed = self.db.claim_enqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
			self.db.commit(tx)
		finally:
			self.db.close(tx)
		self.assertEqual(len(claimed), 1)
		row = self.db.get_workflow(wid)
		self.assertIsNotNone(row)
		self.assertEqual(str(row.get("status")), settings.STATUS_PENDING)
		self.assertIsNone(row.get("started_at_epoch_ms"))
		self.assertIsNone(row.get("deadline_epoch_ms"))
		self.db.release_claim(wid)
		self.db.cancel_workflow(wid, "test_cleanup")

	def test_mark_running_sets_started_and_deadline(self):
		"""mark_running_if_claimed should atomically set RUNNING, started_at, and deadline."""
		rt = getWorkflows(dbName=self.dbName)
		wid = uuid4()
		created = long(now_ms())
		self.db.insert_workflow(
			workflow_id=wid,
			workflow_name="tests.db_contract_mark_running",
			queue_name=settings.QUEUE_DEFAULT,
			partition_key=None,
			priority=0,
			deduplication_id=None,
			application_version=None,
			inputs_obj={"resolved": {}},
			created_ms=created,
			deadline_ms=None,
		)
		tx = self.db.begin()
		try:
			self.db.claim_enqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
			self.db.commit(tx)
		finally:
			self.db.close(tx)

		startedMs = long(now_ms())
		n = self.db.mark_running_if_claimed(
			workflow_id=wid,
			executor_id=rt.executor_id,
			started_ms=startedMs,
			timeout_ms=2000,
		)
		self.assertEqual(int(n), 1)
		row = self.db.get_workflow(wid)
		self.assertIsNotNone(row)
		self.assertEqual(str(row.get("status")), settings.STATUS_RUNNING)
		self.assertEqual(long(row.get("started_at_epoch_ms")), startedMs)
		delta = long(row.get("deadline_epoch_ms") - row.get("started_at_epoch_ms"))
		self.assertEqual(delta, 2000)
		self.db.cancel_workflow(wid, "test_cleanup")

	def test_release_claim_clears_claim_fields(self):
		"""release_claim should clear claim metadata and move row back to ENQUEUED."""
		rt = getWorkflows(dbName=self.dbName)
		wid = uuid4()
		created = long(now_ms())
		self.db.insert_workflow(
			workflow_id=wid,
			workflow_name="tests.db_contract_release_claim",
			queue_name=settings.QUEUE_DEFAULT,
			partition_key=None,
			priority=0,
			deduplication_id=None,
			application_version=None,
			inputs_obj={"resolved": {}},
			created_ms=created,
			deadline_ms=None,
		)
		tx = self.db.begin()
		try:
			self.db.claim_enqueued(settings.QUEUE_DEFAULT, 1, rt.executor_id, tx=tx)
			self.db.commit(tx)
		finally:
			self.db.close(tx)

		n = self.db.release_claim(wid)
		self.assertEqual(int(n), 1)
		row = self.db.get_workflow(wid)
		self.assertIsNotNone(row)
		self.assertEqual(str(row.get("status")), settings.STATUS_ENQUEUED)
		self.assertIsNone(row.get("claimed_by"))
		self.assertIsNone(row.get("claimed_at_epoch_ms"))
		self.assertIsNone(row.get("started_at_epoch_ms"))
		self.assertIsNone(row.get("deadline_epoch_ms"))
		self.db.cancel_workflow(wid, "test_cleanup")

	def test_terminal_guard_blocks_overwrite(self):
		"""update_status_if_not_terminal should not overwrite already terminal rows."""
		wid = uuid4()
		created = long(now_ms())
		self.db.insert_workflow(
			workflow_id=wid,
			workflow_name="tests.db_contract_terminal_guard",
			queue_name=settings.QUEUE_DEFAULT,
			partition_key=None,
			priority=0,
			deduplication_id=None,
			application_version=None,
			inputs_obj={"resolved": {}},
			created_ms=created,
			deadline_ms=None,
		)
		self.db.cancel_workflow(wid, "test_terminal_guard")
		n = self.db.update_status_if_not_terminal(
			workflow_id=wid,
			status=settings.STATUS_SUCCESS,
			fields={"result_json": json.dumps({"bad": True})},
		)
		self.assertEqual(int(n), 0)
		row = self.db.get_workflow(wid)
		self.assertIsNotNone(row)
		self.assertEqual(str(row.get("status")), settings.STATUS_CANCELLED)
