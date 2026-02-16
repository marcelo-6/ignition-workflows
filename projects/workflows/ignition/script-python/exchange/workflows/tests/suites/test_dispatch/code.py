# exchange/workflows/tests/suites/test_dispatch.py
"""Dispatch/claim behavior checks around capacity, partition locking, and claim release."""

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.api import service as service_api
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support import helpers


class TestDispatchSuite(WorkflowTestCase):
	"""Dispatch loop checks that guard queue semantics and local execution safety."""

	SUITE_NAME = "dispatch"

	def test_capacity_limited_claiming(self):
		"""One dispatch tick should not claim more than currently available worker slots."""
		rt = getWorkflows(dbName=self.dbName)
		workers = int(rt._workflowWorkers.get())
		if workers < 1:
			workers = 1

		target = workers + 3
		for i in range(target):
			self.startWorkflow(
				"tests.maintenance_long_running",
				inputsObj={"resolved": {"sleep_s": 6, "heartbeat_ms": 200}},
				queueName=settings.QUEUE_DEFAULT,
				partitionKey="tests.capacity.%d" % i,
				timeoutSeconds=15,
			)

		resp = service_api.dispatch(
			queueName=settings.QUEUE_DEFAULT,
			maxToClaim=target,
			dbName=self.dbName,
			flushMaxItems=0,
			flushMaxMs=1,
		)
		self.assertEnvelope(resp, expectOk=True, expectCode="DISPATCH_OK")
		claimed = int(resp.get("data", {}).get("claimedCount") or 0)
		self.assertLessEqual(claimed, workers, "claimed=%s workers=%s" % (claimed, workers))
		self.assertGreaterEqual(claimed, 1)

	def test_partition_busy_releases_claim(self):
		"""When a partition is locally busy, claimed row should be re-enqueued cleanly."""
		rt = getWorkflows(dbName=self.dbName)
		pkey = "tests.partition.busy.release"
		try:
			rt._activePartitions.add(pkey)
			wid = self.startWorkflow(
				"tests.step_persist_smoke",
				inputsObj={"resolved": {}},
				queueName=settings.QUEUE_DEFAULT,
				partitionKey=pkey,
				timeoutSeconds=10,
			)
			rt.dispatch(queueName=settings.QUEUE_DEFAULT, maxToClaim=1)
			st = self.getStatus(wid)
			self.assertIsNotNone(st)
			self.assertEqual(st.get("status"), settings.STATUS_ENQUEUED)
			self.assertIn(st.get("claimed_by"), (None, ""))
		finally:
			try:
				rt._activePartitions.remove(pkey)
			except:
				pass

	def test_dispatch_failure_releases_claim(self):
		"""If executor rejects dispatch, runtime should release claim back to ENQUEUED."""
		rt = getWorkflows(dbName=self.dbName)
		wid = self.startWorkflow(
			"tests.step_persist_smoke",
			inputsObj={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=10,
		)

		class _FailingQueue(object):
			"""Tiny queue stub so capacity checks can still run."""

			def size(self):
				"""Pretend no queued tasks in this fake executor."""
				return 0

		class _FailingExecutor(object):
			"""Executor stub that always fails execute()."""

			def getActiveCount(self):
				"""Expose zero active workers so claim path is exercised."""
				return 0

			def getQueue(self):
				"""Return queue stub used by runtime metrics."""
				return _FailingQueue()

			def execute(self, runnable):
				"""Force dispatch failure to verify release-claim path."""
				raise Exception("forced dispatch failure for test")

		oldExec = rt._workflowExecutor
		rt._workflowExecutor = _FailingExecutor()
		try:
			rt.dispatch(queueName=settings.QUEUE_DEFAULT, maxToClaim=1)
			st = self.getStatus(wid)
			self.assertIsNotNone(st)
			self.assertEqual(st.get("status"), settings.STATUS_ENQUEUED)
			self.assertIn(st.get("claimed_by"), (None, ""))
		finally:
			rt._workflowExecutor = oldExec

	def test_start_sets_started_and_deadline_at_run(self):
		"""started_at/deadline should be set when run starts, not at enqueue time."""
		wid = self.startWorkflow(
			"tests.cancel_cooperative",
			inputsObj={"resolved": {"sleep_s": 3}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=5.0,
		)
		st0 = self.getStatus(wid)
		self.assertIsNotNone(st0)
		self.assertIsNone(st0.get("started_at_epoch_ms"))
		self.assertIsNone(st0.get("deadline_epoch_ms"))

		self.tickFor(seconds=1.0)
		st1 = self.getStatus(wid)
		self.assertIsNotNone(st1)
		self.assertIsNotNone(st1.get("started_at_epoch_ms"))
		self.assertIsNotNone(st1.get("deadline_epoch_ms"))
		delta = long(st1.get("deadline_epoch_ms") - st1.get("started_at_epoch_ms"))
		self.assertTrue(3000 <= delta <= 7000, "expected ~5s window, got delta_ms=%s" % delta)
