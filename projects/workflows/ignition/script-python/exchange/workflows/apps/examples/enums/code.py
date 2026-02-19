# exchange/workflows/apps/examples/enum.py
# Example enums
# Creates version 1 of each enum if it doesn't already exist.
"""Seed script for demo enums used by workflow parameter templates."""


def _seedEnum(enumName, options, description=None):
	"""
	Create an enum version only when one does not already exist.

	Args:
	    enumName (object): Input value for this call.
	    options (object): Input value for this call.
	    description (object): Input value for this call.

	Returns:
	    None: No explicit value is returned.
	"""
	# Idempotent: only create if no active enum exists
	existing = exchange.workflows.api.params.getEnum(enumName)
	if existing:
		print("SKIP (exists): %s v%s" % (enumName, existing.get("enumVersion")))
		return

	r = exchange.workflows.api.params.createEnumVersion(
		enumName=enumName,
		enumValues=options,
		description=description,
		createdBy="admin",
		activate=True
	)
	print("CREATE:", enumName, r)

def createEnums():
	"""Use this to create example enums in the DB"""
	# 1) Batch/Run modes
	_seedEnum("BatchMode", [
		{"label": "Auto", "value": "AUTO", "isDisabled": False},
		{"label": "Manual", "value": "MANUAL", "isDisabled": False},
		{"label": "Simulate", "value": "SIMULATE", "isDisabled": False}
	], description="High-level batch execution mode.")
	
	# 2) Sampling strategy
	_seedEnum("SampleStrategy", [
		{"label": "Time-based", "value": "TIME_BASED", "isDisabled": False},
		{"label": "Event-based", "value": "EVENT_BASED", "isDisabled": False},
		{"label": "Manual trigger", "value": "MANUAL_TRIGGER", "isDisabled": False}
	], description="How samples are triggered for a workflow/run.")
	
	# 3) Retry policy
	_seedEnum("RetryPolicy", [
		{"label": "None", "value": "NONE", "isDisabled": False},
		{"label": "Linear backoff", "value": "LINEAR", "isDisabled": False},
		{"label": "Exponential backoff", "value": "EXPONENTIAL", "isDisabled": False}
	], description="Retry strategy for recoverable step failures.")
	
	# 4) Priority level
	_seedEnum("PriorityLevel", [
		{"label": "Low", "value": "LOW", "isDisabled": False},
		{"label": "Normal", "value": "NORMAL", "isDisabled": False},
		{"label": "High", "value": "HIGH", "isDisabled": False},
		{"label": "Critical", "value": "CRITICAL", "isDisabled": False}
	], description="Queue/task priority selection.")
	
	# 5) Acknowledgement policy
	_seedEnum("AckPolicy", [
		{"label": "Manual acknowledgement", "value": "MANUAL_ACK", "isDisabled": False},
		{"label": "Auto-acknowledge", "value": "AUTO_ACK", "isDisabled": False}
	], description="How acknowledgements are handled for prompts/alarms/holds.")