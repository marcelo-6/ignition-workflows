# exchange/workflows/tests/suites/test_retention.py
"""Retention smoke checks."""

from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles


class TestRetentionSuite(WorkflowTestCase):
	"""Quick retention suite to make sure cleanup call path stays healthy."""

	SUITE_NAME = "retention"

	@profiles("smoke", "full")
	def test_apply_smoke(self):
		"""applyRetention should run without raising in a normal environment."""
		rt = getWorkflows(dbName=self.dbName)
		rt.applyRetention()
