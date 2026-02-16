def handleTimerEvent():
	from exchange.dbos.engine.instance import get_dbos
	dbos = get_dbos()
	
	# Ensure modules are imported so decorators register workflows
	import exchange.dbos.workflows.demo  # noqa
#	import exchange.dbos.engine.bootstrap
	
	# Run any workflow enqueued on this queue by dispatching on workflow_status.name
	dbos.engine_tick_dispatch(queue_name="default", max_to_claim=10)