ARG IGNITION_VERSION=latest
FROM inductiveautomation/ignition:${IGNITION_VERSION:-latest}

# Copy all third party module files from current build folder (if any)
COPY *.modl /usr/local/bin/ignition/user-lib/modules/
