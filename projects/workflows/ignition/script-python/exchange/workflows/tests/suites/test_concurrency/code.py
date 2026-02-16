# exchange/workflows/tests/suites/test_concurrency.py
"""Concurrency checks around mailbox flow, partition serialization, and cancellation."""

import time

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestConcurrencySuite(WorkflowTestCase):
	"""Runtime concurrency checks that are easier to trust with integration-style tests."""

	SUITE_NAME = "concurrency"

	@profiles("smoke", "full")
	def test_mailbox_hold_resume(self):
		"""Sending HOLD then RESUME should push workflow through HELD and back to completion."""
		rt = getWorkflows(dbName=self.dbName)
		wid = self.startWorkflow(
			"tests.hold_resume",
			inputsObj={"resolved": {"hold_seconds": 3}},
			queueName=settings.QUEUE_DEFAULT,
		)
		self.tickFor(seconds=0.8)
		rt.send(wid, {"cmd": "HOLD"}, topic="cmd")
		time.sleep(0.25)
		rt.send(wid, {"cmd": "RESUME"}, topic="cmd")
		st = self.tickUntilTerminal(wid, timeoutS=12.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)
		helpers.assertEventHistoryContains(self.dbName, wid, "phase.state", "HELD")

	@profiles("smoke", "full")
	def test_partition_serializes_same_key(self):
		"""Two same-partition workflows should not run concurrently in local runtime."""
		wid1 = self.startWorkflow(
			"tests.partition_sleep",
			inputsObj={"resolved": {"sleep_ms": 1500}},
			queueName=settings.QUEUE_DEFAULT,
			partitionKey="unit:TEST",
		)
		wid2 = self.startWorkflow(
			"tests.partition_sleep",
			inputsObj={"resolved": {"sleep_ms": 10}},
			queueName=settings.QUEUE_DEFAULT,
			partitionKey="unit:TEST",
		)
		self.tickFor(seconds=0.5)
		st1 = self.getStatus(wid1)
		st2 = self.getStatus(wid2)
		self.assertIsNotNone(st1)
		self.assertIsNotNone(st2)
		self.assertIn(st2.get("status"), (settings.STATUS_ENQUEUED, settings.STATUS_PENDING))
		st2Term = self.tickUntilTerminal(wid2, timeoutS=12.0)
		self.assertTrue(st2Term is not None and st2Term.get("status") == settings.STATUS_SUCCESS)

	@profiles("smoke", "full")
	def test_streams_log_written(self):
		"""Step persist fixture should leave at least one log row in streams table."""
		wid = self.startWorkflow(
			"tests.step_persist_smoke",
			inputsObj={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
		)
		st = self.tickUntilTerminal(wid, timeoutS=8.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)
		n = helpers.countRows(
			self.dbName,
			"SELECT COUNT(*) FROM workflows.streams WHERE workflow_id=?::uuid AND key='log'",
			[wid],
		)
		self.assertGreaterEqual(int(n), 1)

	@profiles("smoke", "full")
	def test_cancel_cooperative(self):
		"""Manual cancel should push cooperative fixture to CANCELLED terminal state."""
		rt = getWorkflows(dbName=self.dbName)
		wid = self.startWorkflow(
			"tests.cancel_cooperative",
			inputsObj={"resolved": {"sleep_s": 5}},
			queueName=settings.QUEUE_DEFAULT,
		)
		self.tickFor(seconds=0.7)
		rt.cancelWorkflow(wid, reason="test_cancel")
		st = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_CANCELLED)
		self.assertIsNotNone(st.get("error_json"))

	@profiles("smoke", "full")
	def test_timeout_deadline_cancel(self):
		"""Timeout fixture should eventually cancel and persist timeout/cancel metadata."""
		wid = self.startWorkflow(
			"tests.timeout_short",
			inputsObj={"resolved": {"sleep_s": 5}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=1.0,
		)
		self.tickFor(seconds=0.5)
		time.sleep(1.2)
		self.tickFor(seconds=1.5)
		st = helpers.waitTerminal(self.db, wid, timeoutS=10.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_CANCELLED)
		self.assertIsNotNone(st.get("error_json"))
		err = helpers.safeLoads(st.get("error_json")) or {}
		self.assertIn(err.get("type"), ("timeout", "cancelled"))
