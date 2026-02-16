def handleMessage(session, payload):
	exchange.toast.handler.session.addToast(payload)