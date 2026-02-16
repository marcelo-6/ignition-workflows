def onStartup():
	# ignition/timer/tick/onStartup.py
	system.util.getLogger("exchange." + system.util.getProjectName().lower() +".project.onStartup").info("Project '" + system.util.getProjectName().lower() +"' starting... enabling debug level")
	system.util.setLoggingLevel("exchange." + system.util.getProjectName().lower(),"debug")
#	system.util.setLoggingLevel("dbos","debug")