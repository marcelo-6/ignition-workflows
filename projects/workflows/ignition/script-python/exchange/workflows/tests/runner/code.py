# exchange/workflows/tests/runner.py
"""
Unittest-based runner for workflows integration suites.

The runner executes suite modules one by one, captures per-test timing/details,
and returns a structured report that can be rendered as markdown.
"""

import unittest

from exchange.workflows import settings
from exchange.workflows.tests import fixtures
from exchange.workflows.tests.reporting import format_report_md, format_report_text
from exchange.workflows.tests.support import helpers
from exchange.workflows.tests.support.base_case import WorkflowTestCase

log = system.util.getLogger(settings.getLoggerName("tests.runner"))

_SUITE_MODULES = [
	("core", "exchange.workflows.tests.suites.test_core"),
	("api", "exchange.workflows.tests.suites.test_api"),
	("commands", "exchange.workflows.tests.suites.test_commands"),
	("maintenance", "exchange.workflows.tests.suites.test_maintenance"),
	("in_memory_queue", "exchange.workflows.tests.suites.test_in_memory_queue"),
	("dispatch", "exchange.workflows.tests.suites.test_dispatch"),
	("runtime", "exchange.workflows.tests.suites.test_runtime"),
	("steps", "exchange.workflows.tests.suites.test_steps"),
	("concurrency", "exchange.workflows.tests.suites.test_concurrency"),
	("db_contract", "exchange.workflows.tests.suites.test_db"),
	("retention", "exchange.workflows.tests.suites.test_retention"),
]


class _SuiteResult(unittest.TestResult):
	"""Collect per-test timings and outcomes for one suite run."""

	def __init__(self, suiteName):
		unittest.TestResult.__init__(self)
		self.suiteName = suiteName
		self.records = []
		self._startedById = {}

	def startTest(self, test):
		unittest.TestResult.startTest(self, test)
		self._startedById[test.id()] = helpers.nowMs()

	def _record(self, test, status, details=""):
		now = helpers.nowMs()
		testId = test.id()
		startedMs = self._startedById.pop(testId, now)
		methodName = getattr(test, "_testMethodName", testId)
		self.records.append(
			{
				"suite": self.suiteName,
				"name": methodName,
				"id": "%s.%s" % (self.suiteName, methodName),
				"testId": testId,
				"status": status,
				"ok": status == "passed",
				"durationMs": long(now - startedMs),
				"details": details,
			}
		)

	def addSuccess(self, test):
		unittest.TestResult.addSuccess(self, test)
		self._record(test, "passed")

	def addFailure(self, test, err):
		unittest.TestResult.addFailure(self, test, err)
		self._record(test, "failed", details=helpers.formatErr(err))

	def addError(self, test, err):
		unittest.TestResult.addError(self, test, err)
		self._record(test, "error", details=helpers.formatErr(err))

	def addSkip(self, test, reason):
		unittest.TestResult.addSkip(self, test, reason)
		self._record(test, "skipped", details=str(reason or ""))



def _import_module(modulePath):
	"""Import and return one suite module by dotted path."""
	return __import__(modulePath, fromlist=["*"])


def _iter_suite_classes(module):
	"""Yield unittest classes in a suite module."""
	for name in dir(module):
		value = getattr(module, name)
		if isinstance(value, type) and issubclass(value, WorkflowTestCase):
			if value is WorkflowTestCase:
				continue
			if value.__module__ != module.__name__:
				continue
			yield value


def _method_in_profile(method, profile):
	"""Return True if a test method should run in the selected profile."""
	if profile == "full":
		return True
	profiles = helpers.getTestProfiles(method)
	return "smoke" in profiles


def _build_suite(module, suiteName, profile, dbName, runStartedMs):
	"""Build a filtered unittest suite for one suite module."""
	loader = unittest.TestLoader()
	suite = unittest.TestSuite()
	for cls in _iter_suite_classes(module):
		cls.setRunConfig(dbName=dbName, runStartedMs=runStartedMs)
		methodNames = loader.getTestCaseNames(cls)
		for methodName in methodNames:
			method = getattr(cls, methodName)
			if not _method_in_profile(method, profile):
				continue
			suite.addTest(cls(methodName))
	return suite


def _suite_summary(suiteName, records, startedAtMs, endedAtMs):
	"""Build per-suite summary row from recorded case rows."""
	passed = len([r for r in records if r.get("status") == "passed"])
	failed = len([r for r in records if r.get("status") == "failed"])
	errors = len([r for r in records if r.get("status") == "error"])
	skipped = len([r for r in records if r.get("status") == "skipped"])
	total = len(records)
	return {
		"suite": suiteName,
		"startedAtMs": long(startedAtMs),
		"endedAtMs": long(endedAtMs),
		"startedAt": helpers.fmtTs(startedAtMs),
		"endedAt": helpers.fmtTs(endedAtMs),
		"durationMs": long(endedAtMs - startedAtMs),
		"passed": passed,
		"failed": failed,
		"errors": errors,
		"skipped": skipped,
		"total": total,
		"ok": (failed + errors) == 0,
	}


def run_all(dbName=None, profile="full", includeSuites=None, excludeSuites=None):
	"""
	Run workflows unittest suites and return a structured report.

	Args:
		dbName (str|None): Ignition DB connection name.
		profile (str): `full` or `smoke`.
		includeSuites (str|list|tuple|set|None): Optional allow-list.
		excludeSuites (str|list|tuple|set|None): Optional deny-list.

	Returns:
		dict: Full test run report with summary, suite rows, and case rows.
	"""
	dbName = settings.resolveDbName(dbName)
	profile = str(profile or "full").strip().lower()
	if profile not in ("full", "smoke"):
		profile = "full"

	includeSet = helpers.parseSuiteFilter(includeSuites)
	excludeSet = helpers.parseSuiteFilter(excludeSuites)

	helpers.resetRuntimeForTestRun(dbName)
	fixtures.reset_fixtures()

	runStartedMs = helpers.nowMs()
	suiteRows = []
	caseRows = []

	for suiteName, modulePath in _SUITE_MODULES:
		if includeSet is not None and suiteName not in includeSet:
			continue
		if excludeSet is not None and suiteName in excludeSet:
			continue

		suiteStartedMs = helpers.nowMs()
		try:
			module = _import_module(modulePath)
			suite = _build_suite(
				module=module,
				suiteName=suiteName,
				profile=profile,
				dbName=dbName,
				runStartedMs=runStartedMs,
			)
			result = _SuiteResult(suiteName)
			suite.run(result)
			records = result.records
		except Exception as e:
			records = [
				{
					"suite": suiteName,
					"name": "suite_load",
					"id": "%s.suite_load" % suiteName,
					"testId": "%s.suite_load" % suiteName,
					"status": "error",
					"ok": False,
					"durationMs": 0,
					"details": "Failed to import/run suite module %s: %s" % (modulePath, e),
				}
			]
			log.error("suite load failed name=%s module=%s err=%s" % (suiteName, modulePath, e))

		suiteEndedMs = helpers.nowMs()
		caseRows.extend(records)
		suiteRows.append(
			_suite_summary(
				suiteName=suiteName,
				records=records,
				startedAtMs=suiteStartedMs,
				endedAtMs=suiteEndedMs,
			)
		)

	helpers.resetRuntimeForTestRun(dbName)
	swept = 0
	try:
		swept = helpers.sweepStragglers(dbName, runStartedMs)
	except Exception as e:
		log.warn("straggler sweep failed: %s" % e)

	runEndedMs = helpers.nowMs()
	passed = len([r for r in caseRows if r.get("status") == "passed"])
	failed = len([r for r in caseRows if r.get("status") == "failed"])
	errors = len([r for r in caseRows if r.get("status") == "error"])
	skipped = len([r for r in caseRows if r.get("status") == "skipped"])
	total = len(caseRows)

	report = {
		"ok": (failed + errors) == 0,
		"profile": profile,
		"dbName": dbName,
		"startedAtMs": long(runStartedMs),
		"endedAtMs": long(runEndedMs),
		"startedAt": helpers.fmtTs(runStartedMs),
		"endedAt": helpers.fmtTs(runEndedMs),
		"durationMs": long(runEndedMs - runStartedMs),
		"summary": {
			"passed": passed,
			"failed": failed,
			"errors": errors,
			"skipped": skipped,
			"total": total,
			"sweptStragglers": int(swept),
			"suiteCount": len(suiteRows),
			"suiteNames": [row.get("suite") for row in suiteRows],
		},
		"suites": suiteRows,
		"cases": caseRows,
	}
	return report


def run_all_md(dbName=None, profile="full", includeSuites=None, excludeSuites=None):
	"""Run suites and return both raw report and markdown text."""
	report = run_all(
		dbName=dbName,
		profile=profile,
		includeSuites=includeSuites,
		excludeSuites=excludeSuites,
	)
	return {"report": report, "markdown": format_report_md(report)}


__all__ = [
	"run_all",
	"run_all_md",
	"format_report_md",
	"format_report_text",
]
