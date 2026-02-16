# exchange/workflows/tests/support/base_case.py
"""
Base unittest case for workflows integration suites.

This class keeps per-test setup/cleanup consistent and gives each suite a small
set of convenience helpers so test methods stay easy to scan.
"""

import unittest

from exchange.workflows import settings
from exchange.workflows.engine.db import DB
from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.tests.support import helpers


class WorkflowTestCase(unittest.TestCase):
	"""Shared base class for all workflows unittest suites."""

	SUITE_NAME = "misc"
	DB_NAME = settings.DBNAME_DEFAULT
	RUN_STARTED_MS = None

	@classmethod
	def setRunConfig(cls, dbName=None, runStartedMs=None):
		"""Inject run configuration from the test runner."""
		if dbName is not None:
			cls.DB_NAME = settings.resolveDbName(dbName)
		if runStartedMs is not None:
			cls.RUN_STARTED_MS = long(runStartedMs)

	def setUp(self):
		"""Create per-test DB handle and workflow tracking list."""
		self.dbName = settings.resolveDbName(getattr(self.__class__, "DB_NAME", None))
		self.db = DB(self.dbName)
		self.startedWorkflowIds = []

	def tearDown(self):
		"""Best-effort cleanup for workflows started by this test method."""
		helpers.cleanupCase(self.dbName, self.startedWorkflowIds)

	def trackWorkflow(self, workflowId):
		"""Track a workflow id for cleanup at test teardown."""
		if workflowId:
			self.startedWorkflowIds.append(str(workflowId))
		return workflowId

	def startWorkflow(
		self,
		workflowName,
		inputsObj=None,
		queueName=None,
		partitionKey=None,
		priority=0,
		deduplicationId=None,
		applicationVersion=None,
		timeoutSeconds=None,
	):
		"""Enqueue a workflow and auto-track it for cleanup."""
		rt = getWorkflows(dbName=self.dbName)
		queueName = settings.resolveQueueName(queueName)
		timeout = (
			helpers.TEST_WORKFLOW_TIMEOUT_S
			if timeoutSeconds is None
			else float(timeoutSeconds)
		)
		wid = rt.enqueue(
			workflowName=workflowName,
			inputsObj=inputsObj,
			queueName=queueName,
			partitionKey=partitionKey,
			priority=priority,
			deduplicationId=deduplicationId,
			applicationVersion=applicationVersion,
			timeoutSeconds=timeout,
		)
		self.trackWorkflow(wid)
		return wid

	def assertEnvelope(self, resp, expectOk=None, expectCode=None):
		"""Proxy for shared response-envelope assertion helper."""
		helpers.assertEnvelope(resp, expectOk=expectOk, expectCode=expectCode)

	def tickFor(self, seconds=2.0, queueName=None, maxToClaim=10):
		"""Drive dispatch loop for a short amount of time."""
		helpers.tickFor(
			self.dbName,
			seconds=seconds,
			queueName=queueName,
			maxToClaim=maxToClaim,
		)

	def tickUntilTerminal(self, workflowId, timeoutS=10.0, queueName=None, maxToClaim=10):
		"""Drive dispatch + poll until one workflow reaches terminal state."""
		return helpers.tickUntilTerminal(
			self.dbName,
			workflowId,
			timeoutS=timeoutS,
			queueName=queueName,
			maxToClaim=maxToClaim,
		)

	def getStatus(self, workflowId):
		"""Read one workflow status row using shared helper."""
		return helpers.getStatus(self.db, workflowId)
