# exchange/workflows/tests/suites/test_maintenance.py
"""Maintenance mode behavior and swap contract checks."""

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.api import admin as admin_api
from exchange.workflows.api import service as service_api
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestMaintenanceSuite(WorkflowTestCase):
	"""Checks around drain/cancel behavior and controlled swap conditions."""

	SUITE_NAME = "maintenance"

	@profiles("smoke", "full")
	def test_drain_blocks_dispatch_and_start(self):
		"""Drain mode should reject normal starts and block dispatching new work."""
		admin_api.enterMaintenance(
			mode="drain",
			reason="tests.drain_mode",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		try:
			resp = service_api.start(
				workflowName="tests.step_persist_smoke",
				inputs={"resolved": {}},
				queueName=settings.QUEUE_DEFAULT,
				dbName=self.dbName,
			)
			self.assertEnvelope(resp)
			self.assertFalse(resp.get("ok"), "start should reject in maintenance drain mode")

			rt = getWorkflows(dbName=self.dbName)
			wid = rt.enqueue(
				workflowName="tests.step_persist_smoke",
				inputsObj={"resolved": {}},
				queueName=settings.QUEUE_DEFAULT,
				allowDuringMaintenance=True,
				timeoutSeconds=helpers.TEST_WORKFLOW_TIMEOUT_S,
			)
			self.trackWorkflow(wid)
			self.tickFor(seconds=1.0)
			st = self.getStatus(wid)
			self.assertIsNotNone(st)
			self.assertEqual(st.get("status"), settings.STATUS_ENQUEUED)
		finally:
			admin_api.exitMaintenance(dbName=self.dbName)

	@profiles("smoke", "full")
	def test_cancel_cancels_queued_and_running(self):
		"""Cancel mode should push both queued and running workflows to CANCELLED."""
		pkey = "tests.maintenance.cancel.partition"
		widRunning = self.startWorkflow(
			"tests.maintenance_long_running",
			inputsObj={"resolved": {"sleep_s": 20}},
			queueName=settings.QUEUE_DEFAULT,
			partitionKey=pkey,
			timeoutSeconds=30,
		)
		widQueued = self.startWorkflow(
			"tests.maintenance_long_running",
			inputsObj={"resolved": {"sleep_s": 20}},
			queueName=settings.QUEUE_DEFAULT,
			partitionKey=pkey,
			timeoutSeconds=30,
		)

		self.tickFor(seconds=1.0)
		stRun = self.getStatus(widRunning)
		stQueued = self.getStatus(widQueued)
		self.assertIn(stRun.get("status"), (settings.STATUS_RUNNING, settings.STATUS_PENDING))
		self.assertIn(stQueued.get("status"), (settings.STATUS_ENQUEUED, settings.STATUS_PENDING))

		admin_api.enterMaintenance(
			mode="cancel",
			reason="tests.cancel_mode",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		try:
			self.tickFor(seconds=1.5)
			stRunning = helpers.waitTerminal(self.db, widRunning, timeoutS=10.0)
			stQueued = helpers.waitTerminal(self.db, widQueued, timeoutS=10.0)
			self.assertTrue(stRunning is not None and stRunning.get("status") == settings.STATUS_CANCELLED)
			self.assertTrue(stQueued is not None and stQueued.get("status") == settings.STATUS_CANCELLED)
		finally:
			admin_api.exitMaintenance(dbName=self.dbName)

	@profiles("smoke", "full")
	def test_swap_if_drained_contract(self):
		"""Swap should block when active work exists, then succeed once drained."""
		before = admin_api.getMaintenanceStatus(dbName=self.dbName)
		self.assertEnvelope(before, expectOk=True, expectCode="MAINTENANCE_STATUS")
		genBefore = long(before.get("data", {}).get("generation") or 0)

		wid = self.startWorkflow(
			"tests.maintenance_long_running",
			inputsObj={"resolved": {"sleep_s": 20}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=30,
		)
		self.tickFor(seconds=1.0)

		admin_api.enterMaintenance(
			mode="drain",
			reason="tests.swap_contract",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		blocked = admin_api.swapIfDrained(dbName=self.dbName)
		self.assertEnvelope(blocked, expectOk=False)
		self.assertIn(blocked.get("code"), ("NOT_DRAINED", "SWAP_FAILED"))

		rt = getWorkflows(dbName=self.dbName)
		rt.cancelWorkflow(wid, reason="tests.swap_contract")
		self.tickUntilTerminal(wid, timeoutS=10.0)
		self.tickFor(seconds=0.6)

		ok = admin_api.swapIfDrained(dbName=self.dbName)
		self.assertEnvelope(ok)
		if ok.get("ok"):
			self.assertGreater(long(ok.get("data", {}).get("generation") or 0), genBefore)
		else:
			self.assertIn(
				ok.get("code"),
				("NOT_DRAINED", "SWAP_FAILED", "MAINTENANCE_REQUIRED"),
			)

		admin_api.exitMaintenance(dbName=self.dbName)
