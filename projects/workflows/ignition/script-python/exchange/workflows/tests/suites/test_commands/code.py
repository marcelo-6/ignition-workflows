# exchange/workflows/tests/suites/test_commands.py
"""Command policy and mailbox arbitration checks."""

from exchange.workflows import settings
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers
from exchange.workflows.is88 import models as isa88_models


class TestCommandsSuite(WorkflowTestCase):
	"""Small command suite focused on policy and priority rules."""

	SUITE_NAME = "commands"

	@profiles("smoke", "full")
	def test_isa88_policy_matrix(self):
		"""Basic ISA-88 allow-list and command priority order should stay consistent."""
		self.assertTrue(isa88_models.allowedInState("STOP", "RUNNING"))
		self.assertTrue(isa88_models.allowedInState("HOLD", "RUNNING"))
		self.assertTrue(isa88_models.allowedInState("RESUME", "HELD"))
		self.assertTrue(isa88_models.allowedInState("RESET", "ERROR"))
		self.assertFalse(isa88_models.allowedInState("RESUME", "RUNNING"))
		self.assertGreater(
			isa88_models.commandPriority("STOP"),
			isa88_models.commandPriority("HOLD"),
		)
		self.assertGreater(
			isa88_models.commandPriority("HOLD"),
			isa88_models.commandPriority("RESUME"),
		)

		msgs = [{"cmd": "HOLD"}, {"cmd": "RESUME"}, {"cmd": "STOP"}]
		chosen = isa88_models.pickHighestPriority(msgs, state="RUNNING")
		self.assertTrue(isinstance(chosen, dict), "expected chosen command dict")
		self.assertEqual(chosen.get("cmd"), "STOP")

	def test_recv_command_priority(self):
		"""When multiple commands are queued, recvCommand should pick the highest allowed one."""
		rt = getWorkflows(dbName=self.dbName)
		wid = self.startWorkflow(
			"tests.command_priority_probe",
			inputsObj={
				"resolved": {"state": "RUNNING", "wait_s": 2.0, "max_messages": 5}
			},
			queueName=settings.QUEUE_DEFAULT,
		)
		rt.send(wid, {"cmd": "HOLD", "reason": "tests.recv_priority"}, topic="cmd")
		rt.send(wid, {"cmd": "STOP", "reason": "tests.recv_priority"}, topic="cmd")

		st = self.tickUntilTerminal(wid, timeoutS=10.0)
		self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)
		payload = helpers.safeLoads(st.get("result_json")) or {}
		self.assertEqual(payload.get("chosen"), "STOP")
