def handleMessage(session, payload):
	exchange.toast.handler.session.closeToast(**payload)