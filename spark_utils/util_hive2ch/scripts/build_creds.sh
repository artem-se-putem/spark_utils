
#!/bin/bash

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

export OMEGA_USERNAME="'${1}'"
export OMEGA_PASSWORD="'${2}'"
export CH_HOST=${3}
export CH_PORT=${4}
envsubst < ${SCRIPTPATH}/creds_template.py.template > ${SCRIPTPATH}/creds.py