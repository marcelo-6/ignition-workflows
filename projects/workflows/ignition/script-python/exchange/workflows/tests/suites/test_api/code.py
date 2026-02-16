# exchange/workflows/tests/suites/test_api.py
"""API envelope and validation tests for service/admin entry points."""

from exchange.workflows import settings
from exchange.workflows.api import service as service_api
from exchange.workflows.api import admin as admin_api
from exchange.workflows.util import response as response_util
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestApiSuite(WorkflowTestCase):
	"""API-level checks that keep the contract stable for UI callers."""

	SUITE_NAME = "api"

	@profiles("smoke", "full")
	def test_service_envelopes_success(self):
		"""Happy path service calls should return clean envelopes and useful payload data."""
		listResp = service_api.listWorkflows(dbName=self.dbName)
		self.assertEnvelope(listResp, expectOk=True, expectCode="WORKFLOW_OPTIONS_OK")
		self.assertGreaterEqual(listResp.get("data", {}).get("count", 0), 1)

		startResp = service_api.start(
			workflowName="tests.step_persist_smoke",
			inputs={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		self.assertEnvelope(startResp, expectOk=True, expectCode="WORKFLOW_ENQUEUED")
		wid = startResp.get("workflowId") or startResp.get("data", {}).get("workflowId")
		self.assertTrue(wid, "start response should include workflowId")
		self.trackWorkflow(wid)

		sendResp = service_api.sendCommand(
			workflowId=str(wid),
			cmd="STOP",
			reason="tests.service_envelopes_success",
			dbName=self.dbName,
		)
		self.assertEnvelope(sendResp, expectOk=True, expectCode="COMMAND_SENT")

		cancelResp = service_api.cancel(
			workflowId=str(wid),
			reason="tests.service_envelopes_success",
			dbName=self.dbName,
		)
		self.assertEnvelope(cancelResp, expectOk=True, expectCode="CANCEL_REQUESTED")

		dispatchResp = service_api.dispatch(
			queueName=settings.QUEUE_DEFAULT,
			maxToClaim=5,
			dbName=self.dbName,
			flushMaxItems=10,
			flushMaxMs=50,
		)
		self.assertEnvelope(dispatchResp, expectOk=True, expectCode="DISPATCH_OK")

		anchor = helpers.nowMs()
		enqueueResp = service_api.enqueueInMemory(
			workflowName="tests.fast_enqueue_target",
			inputs={"value": "service-envelope"},
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		self.assertEnvelope(enqueueResp)
		if enqueueResp.get("ok"):
			service_api.dispatch(
				queueName=settings.QUEUE_DEFAULT,
				maxToClaim=0,
				dbName=self.dbName,
				flushMaxItems=50,
				flushMaxMs=100,
			)
			for workflowId in helpers.workflowIdsSince(self.dbName, "tests.fast_enqueue_target", anchor):
				self.trackWorkflow(workflowId)

	def test_service_envelopes_validation_errors(self):
		"""Invalid service calls should return stable codes that UI scripts can branch on."""
		resp = service_api.start(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=False, expectCode="INVALID_WORKFLOW_NAME")

		resp = service_api.enqueueInMemory(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=False, expectCode="INVALID_WORKFLOW_NAME")

		resp = service_api.sendCommand(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=False, expectCode="INVALID_ARGUMENTS")

		resp = service_api.cancel(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=False, expectCode="INVALID_ARGUMENTS")

	@profiles("smoke", "full")
	def test_admin_envelopes_success(self):
		"""Admin endpoints should return valid success envelopes in normal operation."""
		resp = admin_api.checkSchema(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=True, expectCode="SCHEMA_OK")

		resp = admin_api.getMaintenanceStatus(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=True, expectCode="MAINTENANCE_STATUS")

		resp = admin_api.applyRetention(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=True, expectCode="RETENTION_APPLIED")

		enter = admin_api.enterMaintenance(
			mode="drain",
			reason="tests.admin_envelopes_success",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		self.assertEnvelope(enter, expectOk=True, expectCode="MAINTENANCE_ENABLED")

		exitResp = admin_api.exitMaintenance(dbName=self.dbName)
		self.assertEnvelope(exitResp, expectOk=True, expectCode="MAINTENANCE_DISABLED")

	def test_admin_envelopes_validation_errors(self):
		"""Admin validation codes should stay stable for obviously bad input."""
		resp = admin_api.enterMaintenance(
			mode="not-a-mode",
			reason="tests.invalid_mode",
			queueName=settings.QUEUE_DEFAULT,
			dbName=self.dbName,
		)
		self.assertEnvelope(resp, expectOk=False, expectCode="INVALID_MODE")

		try:
			admin_api.exitMaintenance(dbName=self.dbName)
		except:
			pass
		resp = admin_api.swapIfDrained(dbName=self.dbName)
		self.assertEnvelope(resp, expectOk=False)
		self.assertIn(resp.get("code"), ("MAINTENANCE_REQUIRED", "SWAP_FAILED"))

	@profiles("smoke", "full")
	def test_response_helper_contract(self):
		"""Response helper should include duration metadata and compatibility top-level data mirror."""
		started = helpers.nowMs() - 5
		ok = response_util.makeOk(
			code="OK_TEST",
			message="ok",
			data={"value": 1},
			meta={"dbName": self.dbName},
			startedMs=started,
		)
		self.assertEnvelope(ok, expectOk=True, expectCode="OK_TEST")
		self.assertEqual(ok.get("value"), 1)
		self.assertIn("durationMs", ok.get("meta", {}))

		err = response_util.makeErr(
			code="ERR_TEST",
			message="bad",
			data={"why": "x"},
			meta={"dbName": self.dbName},
			startedMs=started,
		)
		self.assertEnvelope(err, expectOk=False, expectCode="ERR_TEST")
		self.assertEqual(err.get("why"), "x")
