# exchange/workflows/tests/suites/test_core.py
"""Core sanity checks for registry, schema, kernel contract, and diagnostics."""

from java.util.concurrent import ConcurrentHashMap, ConcurrentLinkedQueue
from java.util.concurrent.atomic import AtomicBoolean, AtomicInteger, AtomicLong

from exchange.workflows.engine.instance import getWorkflows
from exchange.workflows.engine.runtime import WorkflowsRuntime
from exchange.workflows.engine.db import DB
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support import helpers
from exchange.workflows.tests.support.helpers import profiles
import exchange.workflows.engine.instance as instance_mod


class TestCoreSuite(WorkflowTestCase):
	"""Baseline checks that the engine is loaded and wired the way we expect."""

	SUITE_NAME = "core"

	@profiles("smoke", "full")
	def test_registry_fixtures_registered(self):
		"""All fixture workflows and steps should be registered before any suite runs."""
		rt = getWorkflows(dbName=self.dbName)
		workflows = rt.listWorkflows()
		expectedWorkflows = (
			"tests.retry_workflow",
			"tests.hold_resume",
			"tests.command_priority_probe",
			"tests.partition_sleep",
			"tests.cancel_cooperative",
			"tests.timeout_short",
			"tests.step_persist_smoke",
			"tests.replay_error_workflow",
			"tests.replay_success_workflow",
			"tests.nonserializable_result",
			"tests.maintenance_long_running",
			"tests.fast_enqueue_target",
		)
		for workflowName in expectedWorkflows:
			self.assertIn(workflowName, workflows, "missing workflow fixture: %s" % workflowName)

		steps = rt.listSteps()
		expectedSteps = (
			"tests.unstable_step",
			"tests.fast_side_effect_step",
			"tests.always_fail_step",
			"tests.counting_step",
		)
		for stepName in expectedSteps:
			self.assertIn(stepName, steps, "missing step fixture: %s" % stepName)

	@profiles("smoke", "full")
	def test_schema_basic_present(self):
		"""Schema metadata row should exist so behavior tests are not running blind."""
		db = DB(self.dbName)
		version = db.scalar(
			"SELECT value FROM workflows.schema_meta WHERE key='schema_version'", []
		)
		self.assertIsNotNone(version, "schema_meta.schema_version is missing")

	@profiles("smoke", "full")
	def test_kernel_java_only_contract(self):
		"""Kernel should stay Java-only and legacy persisted runtime key should stay empty."""
		rt = getWorkflows(dbName=self.dbName)
		self.assertTrue(isinstance(rt, WorkflowsRuntime), "getWorkflows must return runtime facade")

		kernel = helpers.getKernelOrNone()
		self.assertIsNotNone(kernel, "kernel missing from persistent store")
		self.assertTrue(isinstance(kernel, ConcurrentHashMap), "kernel must be ConcurrentHashMap")
		self.assertTrue(isinstance(kernel.get("generation"), AtomicLong), "generation must be AtomicLong")
		self.assertTrue(
			isinstance(kernel.get("maintenanceEnabled"), AtomicBoolean),
			"maintenanceEnabled must be AtomicBoolean",
		)
		self.assertTrue(
			isinstance(kernel.get("inFlightWorkflows"), AtomicInteger),
			"inFlightWorkflows must be AtomicInteger",
		)
		self.assertTrue(
			isinstance(kernel.get("inMemoryQueue"), ConcurrentLinkedQueue),
			"inMemoryQueue must be ConcurrentLinkedQueue",
		)
		self.assertIsNotNone(kernel.get("wfExecutor"), "wfExecutor missing")
		self.assertIsNotNone(kernel.get("stepExecutor"), "stepExecutor missing")

		store, _storeType = instance_mod._getStoreMap()
		legacy = instance_mod._getStore(store, instance_mod._OLD_RUNTIME_KEY)
		self.assertIsNone(legacy, "legacy persisted runtime key must stay empty")

	@profiles("smoke", "full")
	def test_runtime_ephemeral_facade(self):
		"""Two getWorkflows calls should return different facades over the same kernel."""
		rt1 = getWorkflows(dbName=self.dbName)
		rt2 = getWorkflows(dbName=self.dbName)
		self.assertIsNot(rt1, rt2, "expected ephemeral facades")
		self.assertIs(rt1.kernel, rt2.kernel, "facades should share one kernel")
		self.assertEqual(rt1.executor_id, rt2.executor_id)

	@profiles("smoke", "full")
	def test_apps_load_idempotent(self):
		"""Calling apps.load repeatedly should stay idempotent and not explode."""
		import exchange.workflows.apps as apps

		a = apps.__init__.load(include_tests=True)
		b = apps.__init__.load(include_tests=True)
		self.assertTrue(a is True and b is True, "apps.load should be idempotent")

	@profiles("smoke", "full")
	def test_executor_diagnostics_shape(self):
		"""Diagnostics payload should include all top sections used by admin UI."""
		rt = getWorkflows(dbName=self.dbName)
		diag = rt.getExecutorDiagnostics()
		for key in ("identity", "maintenance", "workload", "executors", "inMemoryQueue", "capacity"):
			self.assertIn(key, diag, "diagnostics missing section: %s" % key)
		self.assertIn("workflows", diag.get("executors", {}), "missing workflow executor stats")
		self.assertIn("steps", diag.get("executors", {}), "missing step executor stats")
