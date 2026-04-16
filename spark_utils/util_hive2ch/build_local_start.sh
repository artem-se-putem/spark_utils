#!/bin/bash

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

rm ${SCRIPTPATH}/local_start.sh

export IPA_USERNAME=${1}
export KEYTAB_PATH=${2}
export SPARK_YARN_QUEUE=${3}
export PYTHON_EXE_PATH=${4}
export SPARK_INSTANCES=${5}
export SPARK_EXECUTOR_CORES=${6}
export SPARK_EXECUTOR_MEMORY=${7}
export SPARK_EXECUTOR_MEMORY_OVERHEAD=${8}
export SPARK_DRIVER_CORES=${9}
export SPARK_DRIVER_MEMORY=${10}
export SPARK_DRIVER_MEMORY_OVERHEAD=${11}
export ARGS=${12}
export LOG4J_CFG=${13}
export SPARK_APP_NAME=${14}
export PYTHON_PATH=${15}
envsubst < ${SCRIPTPATH}/local_start.sh.template > ${SCRIPTPATH}/local_start.sh