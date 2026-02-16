# exchange/workflows/tests/reporting.py
"""
Report formatters for the unittest-based workflows test runner.

These helpers keep markdown/text rendering out of the runner logic so updates to
report style are easy and local.
"""


def _safe(value, fallback=""):
	"""Return a printable value for report output."""
	if value is None:
		return fallback
	try:
		return str(value)
	except:
		return fallback


def format_report_text(report):
	"""Render a plain-text report for quick script-console checks."""
	summary = report.get("summary", {})
	lines = []
	lines.append(
		"Workflows Tests (%s): %s"
		% (_safe(report.get("profile"), "full"), "PASS" if report.get("ok") else "FAIL")
	)
	lines.append("Started: %s" % _safe(report.get("startedAt")))
	lines.append("Ended:   %s" % _safe(report.get("endedAt")))
	lines.append("Duration(ms): %s" % _safe(report.get("durationMs"), "0"))
	lines.append(
		"Passed: %s  Failed: %s  Errors: %s  Skipped: %s  Total: %s  Swept: %s"
		% (
			_safe(summary.get("passed"), "0"),
			_safe(summary.get("failed"), "0"),
			_safe(summary.get("errors"), "0"),
			_safe(summary.get("skipped"), "0"),
			_safe(summary.get("total"), "0"),
			_safe(summary.get("sweptStragglers"), "0"),
		)
	)
	lines.append("")

	suites = report.get("suites", [])
	if suites:
		lines.append("Suites:")
		for suite in suites:
			lines.append(
				"- %s: %s passed / %s failed / %s errors / %s skipped (%s ms)"
				% (
					_safe(suite.get("suite")),
					_safe(suite.get("passed"), "0"),
					_safe(suite.get("failed"), "0"),
					_safe(suite.get("errors"), "0"),
					_safe(suite.get("skipped"), "0"),
					_safe(suite.get("durationMs"), "0"),
				)
			)
		lines.append("")

	for case in report.get("cases", []):
		lines.append(
			"[%s] %s (%s ms)"
			% (
				_safe(case.get("status"), "unknown").upper(),
				_safe(case.get("id")),
				_safe(case.get("durationMs"), "0"),
			)
		)
		if case.get("status") in ("failed", "error"):
			lines.append(case.get("details") or "")
			lines.append("")

	return "\n".join(lines)


def format_report_md(report):
	"""Render a markdown report for Perspective panels and saved artifacts."""
	summary = report.get("summary", {})
	lines = []
	lines.append(
		"## Workflows Tests (%s) - %s"
		% (_safe(report.get("profile"), "full"), "PASSED" if report.get("ok") else "FAILED")
	)
	lines.append("")
	lines.append("- Full Suite Started: **%s**" % _safe(report.get("startedAt")))
	lines.append("- Full Suite Ended: **%s**" % _safe(report.get("endedAt")))
	lines.append("- Duration: **%s ms**" % _safe(report.get("durationMs"), "0"))
	lines.append("- Passed: **%s**" % _safe(summary.get("passed"), "0"))
	lines.append("- Failed: **%s**" % _safe(summary.get("failed"), "0"))
	lines.append("- Errors: **%s**" % _safe(summary.get("errors"), "0"))
	lines.append("- Skipped: **%s**" % _safe(summary.get("skipped"), "0"))
	lines.append("- Total: **%s**" % _safe(summary.get("total"), "0"))
	lines.append("- Swept Stragglers: **%s**" % _safe(summary.get("sweptStragglers"), "0"))
	lines.append("")

	suites = report.get("suites", [])
	if suites:
		lines.append("### Suite Summary")
		lines.append("| Suite | Passed | Failed | Errors | Skipped | Total | Duration (ms) |")
		lines.append("|---|---:|---:|---:|---:|---:|---:|")
		for suite in suites:
			lines.append(
				"| `%s` | %s | %s | %s | %s | %s | %s |"
				% (
					_safe(suite.get("suite")),
					_safe(suite.get("passed"), "0"),
					_safe(suite.get("failed"), "0"),
					_safe(suite.get("errors"), "0"),
					_safe(suite.get("skipped"), "0"),
					_safe(suite.get("total"), "0"),
					_safe(suite.get("durationMs"), "0"),
				)
			)
		lines.append("")

	lines.append("### Cases")
	lines.append("| Result | Suite | Case | Duration (ms) |")
	lines.append("|---|---|---|---:|")
	for case in report.get("cases", []):
		lines.append(
			"| %s | `%s` | `%s` | %s |"
			% (
				_safe(case.get("status"), "unknown").upper(),
				_safe(case.get("suite")),
				_safe(case.get("name")),
				_safe(case.get("durationMs"), "0"),
			)
		)
	lines.append("")

	slowest = sorted(
		report.get("cases", []),
		key=lambda row: int(row.get("durationMs") or 0),
		reverse=True,
	)[:8]
	if slowest:
		lines.append("### Slowest Cases")
		for row in slowest:
			lines.append(
				"- `%s.%s` - %s ms"
				% (
					_safe(row.get("suite")),
					_safe(row.get("name")),
					_safe(row.get("durationMs"), "0"),
				)
			)
		lines.append("")

	failures = [
		row
		for row in report.get("cases", [])
		if row.get("status") in ("failed", "error")
	]
	if failures:
		lines.append("### Failures")
		for row in failures:
			lines.append("#### `%s.%s`" % (_safe(row.get("suite")), _safe(row.get("name"))))
			lines.append("```")
			lines.append((row.get("details") or "")[:16000])
			lines.append("```")
			lines.append("")

	return "\n".join(lines)
