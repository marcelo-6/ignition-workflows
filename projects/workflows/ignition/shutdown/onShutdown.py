def onShutdown():
	system.util.getLogger("exchange." + system.util.getProjectName().lower() +".project.onShutdown").info("Project '" + system.util.getProjectName().lower() +"' shutdown")
#	"""
#	Gateway shutdown hook: best-effort shutdown of DBOS runtime singleton.
#	Prefers globalVarMap storage (if available), falls back to getGlobals().
#	""" #
#	
#	try:
#	    # Use the same storage selection logic as instance.py
#	    store, store_kind = exchange.dbos.engine.instance._get_store()
#	    key = exchange.dbos.engine.instance._GLOBAL_KEY
#	
#	    rt = exchange.dbos.engine.instance._store_get(store, key)
#	    if rt is not None and hasattr(rt, "shutdown"):
#	        try:
#	            rt.shutdown()
#	            system.util.getLogger("dbos.engine.instance").warn("onShutdown")
#	        except Exception as e:
#	            system.util.getLogger("dbos.engine.instance").warn(
#	                "Error during DBOS shutdown [store=%s]: %s" % (store_kind, e)
#	            )
#	
#	    # Clear reference in the active store
#	    exchange.dbos.engine.instance._store_del(store, key)
#	    exchange.dbos.engine.instance._store_refresh(store)
#	
#	    # Also clear getGlobals() as a safety net (in case storage mode changed)
#	    try:
#	        g = system.util.getGlobals()
#	        if key in g:
#	            g[key] = None
#	    except Exception:
#	        pass
#	
#	except Exception as e:
#	    system.util.getLogger("dbos.engine.instance").warn(
#	        "onShutdown failed: %s" % e
#	    )