def handleMessage(session, payload):
	session.custom.maintenance.enabled = payload["status"]