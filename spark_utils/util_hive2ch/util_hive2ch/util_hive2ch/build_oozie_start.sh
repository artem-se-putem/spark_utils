#!/bin/bash

SCRIPT=$(realpath	"$0")
SCRIPTPATH=$(dirname	"$SCRIPT")

export	IPA_USERNAME=${1}
export	KEYTAB_PATH=${2}
export	SPARK_YARN_QUEUE=${3}
export	PYTHON_EXE_PATH=${4}
export	SPARK_INSTANCES=${5}
export	SPARK_EXECUTOR_CORES=${6}
export	SPARK_EXECUTOR_MEMORY=${7}
export	SPARK_EXECUTOR_MEMORY_OVERHEAD=${8}
export	SPARK_DRIVER_CORES=${9}
export	SPARK_DRIVER_MEMORY=${10}
export	SPARK_DRIVER_MEMORY_OVERHEAD=${11}
export	LOG4J_CFG=${12}
export	SPARK_APP_NAME=${13}
export	PYTHON_PATH=${14}
envsubst	<	${SCRIPTPATH}/oozie_start.sh.template	>	${SCRIPTPATH}/oozie_start.sh