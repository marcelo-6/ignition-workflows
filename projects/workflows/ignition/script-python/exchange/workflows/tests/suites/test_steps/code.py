# exchange/workflows/tests/suites/test_steps.py
"""Step persistence, retry, and replay behavior checks."""

from exchange.workflows import settings
import exchange.workflows.tests.fixtures as fixtures
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestStepsSuite(WorkflowTestCase):
	"""Step-focused checks for output persistence, retries, and replay semantics."""

	SUITE_NAME = "steps"

	@profiles("smoke", "full")
	def test_operation_outputs_persisted(self):
		"""One successful step call should create operation_outputs row with SUCCESS status."""
		wid = self.startWorkflow(
			"tests.step_persist_smoke",
			inputsObj={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
		)
		st = self.tickUntilTerminal(wid, timeoutS=8.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)

		outs = helpers.getOperationOutputs(self.dbName, wid)
		self.assertGreaterEqual(len(outs), 1, "expected at least one operation output row")
		self.assertEqual(outs[0].get("status"), settings.STATUS_SUCCESS)
		self.assertEqual(int(outs[0].get("attempts")), 1)

	@profiles("smoke", "full")
	def test_retry_policy_dbos(self):
		"""Retry fixture should succeed and record attempts at or above failure threshold."""
		fixtures.reset_fixtures()
		wid = self.startWorkflow(
			"tests.retry_workflow",
			inputsObj={"resolved": {"fail_until": 4}},
			queueName=settings.QUEUE_DEFAULT,
		)
		st = self.tickUntilTerminal(wid, timeoutS=12.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)

		outs = helpers.getOperationOutputs(self.dbName, wid)
		self.assertGreaterEqual(len(outs), 1)
		self.assertEqual(outs[0].get("status"), settings.STATUS_SUCCESS)
		self.assertGreaterEqual(int(outs[0].get("attempts")), 4)

	def test_error_replay_does_not_rerun(self):
		"""Cached ERROR replay should fail deterministically without rerunning step body."""
		fixtures.reset_fixtures()
		wid = self.startWorkflow(
			"tests.replay_error_workflow",
			inputsObj={"resolved": {"marker": "first"}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=20,
		)
		st1 = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st1 is not None and st1.get("status") == settings.STATUS_ERROR)
		callsBefore = fixtures.get_counter("always_fail_step")
		self.assertEqual(int(callsBefore), 1)

		rows = helpers.forceReenqueue(self.dbName, wid)
		self.assertEqual(int(rows), 1)
		st2 = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st2 is not None and st2.get("status") == settings.STATUS_ERROR)
		callsAfter = fixtures.get_counter("always_fail_step")
		self.assertEqual(int(callsAfter), int(callsBefore))
		err = helpers.safeLoads(st2.get("error_json")) or {}
		message = str(err.get("message") or "")
		self.assertTrue("Replayed step error" in message)

	def test_success_replay_does_not_rerun(self):
		"""Cached SUCCESS replay should skip rerunning the underlying step function."""
		fixtures.reset_fixtures()
		wid = self.startWorkflow(
			"tests.replay_success_workflow",
			inputsObj={"resolved": {"payload": "first"}},
			queueName=settings.QUEUE_DEFAULT,
			timeoutSeconds=20,
		)
		st1 = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st1 is not None and st1.get("status") == settings.STATUS_SUCCESS)
		callsBefore = fixtures.get_counter("counting_step")
		self.assertEqual(int(callsBefore), 1)

		rows = helpers.forceReenqueue(self.dbName, wid)
		self.assertEqual(int(rows), 1)
		st2 = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st2 is not None and st2.get("status") == settings.STATUS_SUCCESS)
		callsAfter = fixtures.get_counter("counting_step")
		self.assertEqual(int(callsAfter), int(callsBefore))

	def test_nonserializable_result_fallback(self):
		"""Non-JSON workflow result should store serializer fallback payload instead of crashing."""
		wid = self.startWorkflow(
			"tests.nonserializable_result",
			inputsObj={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
		)
		st = self.tickUntilTerminal(wid, timeoutS=8.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)
		resultObj = helpers.safeLoads(st.get("result_json")) or {}
		self.assertEqual(resultObj.get("type"), "result_serialize_error")
