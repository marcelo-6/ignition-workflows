# exchange/workflows/tests/suites/test_runtime.py
"""Runtime-level integration checks that don't fit neatly in other suites."""

from exchange.workflows import settings
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles


class TestRuntimeSuite(WorkflowTestCase):
	"""Small runtime suite focused on basic enqueue/execute behavior."""

	SUITE_NAME = "runtime"

	@profiles("smoke", "full")
	def test_start_enqueues_row(self):
		"""enqueue should create workflow row and complete once dispatch ticks run."""
		wid = self.startWorkflow(
			"tests.step_persist_smoke",
			inputsObj={"resolved": {}},
			queueName=settings.QUEUE_DEFAULT,
		)
		st = self.getStatus(wid)
		self.assertIsNotNone(st, "workflow_status row missing")
		self.assertIn(
			st.get("status"),
			(
				settings.STATUS_ENQUEUED,
				settings.STATUS_PENDING,
				settings.STATUS_RUNNING,
				settings.STATUS_SUCCESS,
			),
		)
		st2 = self.tickUntilTerminal(wid, timeoutS=8.0)
		self.assertTrue(st2 is not None and st2.get("status") == settings.STATUS_SUCCESS)
		self.assertIsNotNone(st2.get("deadline_epoch_ms"), "deadline should be set once execution starts")
