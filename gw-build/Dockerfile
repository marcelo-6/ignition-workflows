ARG IGNITION_VERSION=latest
FROM inductiveautomation/ignition:${IGNITION_VERSION:-latest}

# Copy all third party module files from current build folder (if any)
COPY *.modl /usr/local/bin/ignition/user-lib/modules/

# Notes, I recommend these 
# com.automation_pros.simaids
# I mainly use it for the ConcurrentHashMap function
# https://forum.inductiveautomation.com/t/automation-professionals-integration-toolkit-module

# com.mussonindustrial.embr.periscope
# I mainly use it for the Toast notifications and runJavaScriptAsync functions
# https://forum.inductiveautomation.com/t/musson-industrials-embr-periscope-module

# project-scan-endpoint
# https://github.com/bw-design-group/ignition-project-scan-endpoint

# You need the latest .modl files in the gw-build folder either when you first 
# pull and build your docker image locally or you can just import in the gateway webpage

# Keep in mind as of 8.3, if you set GATEWAY_MODULES_ENABLED as a subset of all the modules
# you wont be able to use the webpage to add new modules you can still to add the files from the 
# webpage but cannot enable them from there. You NEED to add to GATEWAY_MODULES_ENABLED. 
# See .env.example
