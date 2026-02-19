# exchange/workflows/tests/suites/test_in_memory_queue.py
"""In-memory queue behavior checks: enqueue, maintenance, overflow, flush limits."""

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.db import uuid4
from exchange.workflows.api import admin as admin_api
from exchange.workflows.api import service as service_api
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestInMemoryQueueSuite(WorkflowTestCase):
	"""Queue-focused tests for fast in-memory ingress before DB flush."""

	SUITE_NAME = "in_memory_queue"

	@profiles("smoke", "full")
	def test_enqueue_and_timer_flush_roundtrip(self):
		"""enqueueInMemory should avoid direct DB write, then flush + run on dispatch."""
		beforeCount = helpers.countRows(
			self.dbName,
			"SELECT COUNT(*) FROM workflows.workflow_status WHERE workflow_name=?",
			["tests.fast_enqueue_target"],
		)
		anchor = helpers.nowMs()
		ack = service_api.enqueueInMemory(
			workflowName="tests.fast_enqueue_target",
			inputs={"value": "hello-fast"},
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		self.assertEnvelope(ack)
		self.assertTrue(ack.get("ok") is True, "enqueueInMemory should accept in normal mode")

		middleCount = helpers.countRows(
			self.dbName,
			"SELECT COUNT(*) FROM workflows.workflow_status WHERE workflow_name=?",
			["tests.fast_enqueue_target"],
		)
		self.assertEqual(middleCount, beforeCount, "enqueueInMemory should not insert DB row immediately")

		service_api.dispatch(
			queueName=settings.QUEUE_DEFAULT,
			maxToClaim=10,
			dbName=self.dbName,
			flushMaxItems=100,
			flushMaxMs=200,
		)
		wid = helpers.latestWorkflowId(
			self.dbName,
			"tests.fast_enqueue_target",
			createdAfterMs=anchor,
		)
		self.assertIsNotNone(wid, "expected flushed workflow row")
		self.trackWorkflow(wid)
		st = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)

	@profiles("smoke", "full")
	def test_maintenance_rejects_by_default(self):
		"""By default, maintenance mode should reject new in-memory enqueue requests."""
		admin_api.enterMaintenance(
			mode="drain",
			reason="tests.in_memory_maintenance",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		try:
			ack = service_api.enqueueInMemory(
				workflowName="tests.fast_enqueue_target",
				inputs={"value": "blocked"},
				queueName=settings.QUEUE_DEFAULT,
				dbName=self.dbName,
			)
			self.assertEnvelope(ack, expectOk=False)
			self.assertEqual(ack.get("code"), "IN_MEMORY_ENQUEUE_REJECTED")
			self.assertEqual(ack.get("data", {}).get("reason"), "maintenance")
		finally:
			admin_api.exitMaintenance(dbName=self.dbName)

	def test_overflow_rejects_and_counts(self):
		"""Forcing low max depth should trigger overflow rejection and increment counter."""
		rt = getWorkflows(dbName=self.dbName)
		oldMax = int(rt._inMemoryQueueMaxDepth.get())
		oldOverflow = long(rt._inMemoryQueueOverflowCount.get())
		anchor = helpers.nowMs()

		while True:
			item = rt._inMemoryQueue.poll()
			if item is None:
				break

		rt._inMemoryQueueMaxDepth.set(1)
		try:
			admin_api.enterMaintenance(
				mode="drain",
				reason="tests.in_memory_queue_overflow",
				queueName=settings.QUEUE_DEFAULT,
				dbName=self.dbName,
			)
			ack1 = service_api.enqueueInMemory(
				workflowName="tests.fast_enqueue_target",
				inputs={"value": "bp-1"},
				queueName=settings.QUEUE_DEFAULT,
				allowDuringMaintenance=True,
				dbName=self.dbName,
			)
			self.assertEnvelope(ack1, expectOk=True, expectCode="IN_MEMORY_ENQUEUE_ACCEPTED")

			ack2 = service_api.enqueueInMemory(
				workflowName="tests.fast_enqueue_target",
				inputs={"value": "bp-2"},
				queueName=settings.QUEUE_DEFAULT,
				allowDuringMaintenance=True,
				dbName=self.dbName,
			)
			self.assertEnvelope(ack2, expectOk=False, expectCode="IN_MEMORY_ENQUEUE_REJECTED")
			self.assertEqual(ack2.get("data", {}).get("reason"), "overflow")

			status = admin_api.getMaintenanceStatus(dbName=self.dbName)
			self.assertEnvelope(status, expectOk=True)
			overflowCount = long(status.get("data", {}).get("inMemoryQueueOverflowCount") or 0)
			self.assertGreaterEqual(overflowCount, oldOverflow + 1)
		finally:
			rt._inMemoryQueueMaxDepth.set(oldMax)
			try:
				rt.flushInMemoryQueue(maxItems=200, maxMs=200, allowDuringMaintenance=True)
			except:
				pass
			try:
				admin_api.exitMaintenance(dbName=self.dbName)
			except:
				pass
			for workflowId in helpers.workflowIdsSince(self.dbName, "tests.fast_enqueue_target", anchor):
				self.trackWorkflow(workflowId)

	def test_flush_respects_max_items(self):
		"""flushInMemoryQueue should honor maxItems and leave leftovers for next flush."""
		anchor = helpers.nowMs()
		rt = getWorkflows(dbName=self.dbName)
		admin_api.enterMaintenance(
			mode="drain",
			reason="tests.in_memory_queue_flush_max_items",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		try:
			for i in range(3):
				resp = service_api.enqueueInMemory(
					workflowName="tests.fast_enqueue_target",
					inputs={"value": "batch-%d" % i},
					queueName=settings.QUEUE_DEFAULT,
					allowDuringMaintenance=True,
					dbName=self.dbName,
				)
				self.assertEnvelope(resp, expectOk=True)

			inserted = int(rt.flushInMemoryQueue(maxItems=2, maxMs=200, allowDuringMaintenance=True))
			self.assertEqual(inserted, 2)

			inserted2 = int(rt.flushInMemoryQueue(maxItems=20, maxMs=200, allowDuringMaintenance=True))
			self.assertGreaterEqual(inserted2, 1)
		finally:
			admin_api.exitMaintenance(dbName=self.dbName)

		ids = helpers.workflowIdsSince(self.dbName, "tests.fast_enqueue_target", anchor)
		for workflowId in ids:
			self.trackWorkflow(workflowId)
		self.assertGreaterEqual(len(ids), 3)

	@profiles("full")
	def test_in_memory_flush_dedup_failure_does_not_poison_queue(self):
		"""Persistent dedupe insert failures should not block good payloads from flushing."""
		rt = getWorkflows(dbName=self.dbName)
		queueName = "tests.in_memory.poison.%s" % long(helpers.nowMs())
		blockerDedup = "tests.in_memory.poison.blocker.%s" % long(helpers.nowMs())
		goodDedup = blockerDedup + ".good"

		blockerWid = uuid4()
		self.db.insertWorkflow(
			workflowId=blockerWid,
			workflowName="tests.fast_enqueue_target",
			queueName=queueName,
			partitionKey=None,
			priority=0,
			deduplicationId=blockerDedup,
			applicationVersion=None,
			inputsObj={"resolved": {"value": "blocker"}},
			createdMs=long(helpers.nowMs()),
			deadlineMs=None,
		)
		self.trackWorkflow(blockerWid)

		badAck = service_api.enqueueInMemory(
			workflowName="tests.fast_enqueue_target",
			inputs={"value": "bad"},
			queueName=queueName,
			deduplicationId=blockerDedup,
			dbName=self.dbName,
		)
		self.assertEnvelope(badAck, expectOk=True, expectCode="IN_MEMORY_ENQUEUE_ACCEPTED")

		goodAck = service_api.enqueueInMemory(
			workflowName="tests.fast_enqueue_target",
			inputs={"value": "good"},
			queueName=queueName,
			deduplicationId=goodDedup,
			dbName=self.dbName,
		)
		self.assertEnvelope(goodAck, expectOk=True, expectCode="IN_MEMORY_ENQUEUE_ACCEPTED")

		inserted = int(rt.flushInMemoryQueue(maxItems=20, maxMs=500))
		self.assertEqual(
			inserted,
			1,
			"expected good payload to flush even when bad dedupe payload fails",
		)

		goodRows = helpers.countRows(
			self.dbName,
			"SELECT COUNT(*) FROM workflows.workflow_status WHERE queue_name=? AND deduplication_id=?",
			[queueName, goodDedup],
		)
		self.assertEqual(goodRows, 1)

		maxRetries = int(getattr(settings, "FAST_FLUSH_FAILURE_MAX_RETRIES", 3))
		for _i in range(maxRetries + 1):
			rt.flushInMemoryQueue(maxItems=20, maxMs=500)

		diag = rt.getExecutorDiagnostics()
		inMemory = (diag or {}).get("inMemoryQueue", {}) or {}
		self.assertEqual(int(inMemory.get("depth") or 0), 0)
		self.assertGreaterEqual(int(inMemory.get("deadLetterCount") or 0), 1)
		self.assertGreaterEqual(int(inMemory.get("deadLetterDepth") or 0), 1)

		ds = system.db.runPrepQuery(
			"SELECT workflow_id FROM workflows.workflow_status WHERE queue_name=?",
			[queueName],
			self.dbName,
		)
		for r in range(ds.getRowCount()):
			self.trackWorkflow(str(ds.getValueAt(r, 0)))
