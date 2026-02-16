def handleMessage(session, payload):
	exchange.toast.handler.session.clearToast(**payload)