function _InitVariables {
	export GENERATE_DDL_ARGS_APP="--schema_ch ${CH_SCHEMA} \\
--distributed_table ${CH_DISTRIBUTED_TABLE} \\
--replicated_table ${CH_MERGETREE_TABLE} \\
--schema_hive ${HIVE_SCHEMA} \\
--hive_table ${HIVE_TABLE} \\
--sharding_key_ch ${CH_PARTITION_BY_FIELD} \\
--cluster ${CH_CLUSTER}"

if [[ "${INCREMENT}" -eq 0 ]]; then
		export LOCAL_ARGS_APP="--job_name ${JOB_NAME} \\
--increment ${INCREMENT} \\
--schema_ch ${CH_SCHEMA} \\
--distributed_table ${CH_DISTRIBUTED_TABLE} \\
--replicated_table ${CH_MERGETREE_TABLE} \\
--schema_hive ${HIVE_SCHEMA} \\
--hive_table ${HIVE_TABLE} \\
--sharding_key_ch ${CH_PARTITION_BY_FIELD} \\
--cluster ${CH_CLUSTER}"
	elif [[ "${INCREMENT}" -eq 1 ]]; then
		export LOCAL_ARGS_APP="--job_name ${JOB_NAME} \\
--increment ${INCREMENT} \\
--save_interval ${SAVE_INTERVAL} \\
--schema_ch ${CH_SCHEMA} \\
--distributed_table ${CH_DISTRIBUTED_TABLE} \\
--replicated_table ${CH_MERGETREE_TABLE} \\
--schema_hive ${HIVE_SCHEMA} \\
--hive_table ${HIVE_TABLE} \\
--sharding_key_ch ${CH_PARTITION_BY_FIELD} \\
--cluster ${CH_CLUSTER}"
	elif [[ "${INCREMENT}" -eq 2 ]] || [[ "${INCREMENT}" -eq 3 ]]; then
		export LOCAL_ARGS_APP="--job_name ${JOB_NAME} \\
--increment ${INCREMENT} \\
--start_date ${START_DATE} \\
--end_date ${END_DATE} \\
--schema_ch ${CH_SCHEMA} \\
--distributed_table ${CH_DISTRIBUTED_TABLE} \\
--replicated_table ${CH_MERGETREE_TABLE} \\
--schema_hive ${HIVE_SCHEMA} \\
--hive_table ${HIVE_TABLE} \\
--sharding_key_ch ${CH_PARTITION_BY_FIELD} \\
--cluster ${CH_CLUSTER}"
	fi
}

function ReadEnvFile {
	while IFS= read -r line; do
		[[ -z "$line" || $line =~ ^# ]] && continue
		eval "export $line"
	done < "./common.env"

	envsubst <./tech.env.template>./tech.env

	while IFS= read -r line; do
		[[ -z "$line" || $line =~ ^# ]] && continue
		eval "export $line"
	done < "./tech.env"

	_InitVariables
}
ReadEnvFile

function keytab_init {
	/usr/sbin/ipa-getkeytab -P -p ${IPA_USERNAME}\@DF.SBRF.RU -k ~/${IPA_USERNAME}\.keytab ${IPA_USERNAME}
	kinit ${IPA_USERNAME}@DF.SBRF.RU -k -t /home/${IPA_USERNAME}/${IPA_USERNAME}.keytab
	mv ~/${IPA_USERNAME}.keytab ~/.auth/${IPA_USERNAME}.keytab
}

function LocalStartBuild {
	bash "${ROOT_PATH}"/build_local_start.sh "${IPA_USERNAME}" "${LINUX_LOCAL_PATH_KEYTAB}" "${SPARK_YARN_QUEUE}" "${PYTHON_EXE_PATH}" "${SPARK_INSTANCES}" "${SPARK_EXECUTOR_CORES}" "${SPARK_EXECUTOR_MEMORY}" "${SPARK_EXECUTOR_MEMORY_OVERHEAD}" "${SPARK_DRIVER_CORES}" "${SPARK_DRIVER_MEMORY}" "${SPARK_DRIVER_MEMORY_OVERHEAD}" "${LOCAL_ARGS_APP}" ${LOG4J_CFG} "${SPARK_APP_NAME}"
}

function OozieStartBuild {
	bash	"${ROOT_PATH}"/build_oozie_start.sh	"${IPA_USERNAME}"	"${OOZIE_CONTAINER_PATH_KEYTAB}"	"${SPARK_YARN_QUEUE}"	"${PYTHON_EXE_PATH}"	"${SPARK_INSTANCES}"	"${SPARK_EXECUTOR_CORES}"	"${SPARK_EXECUTOR_MEMORY}"	"${SPARK_EXECUTOR_MEMORY_OVERHEAD}"	"${SPARK_DRIVER_CORES}"	"${SPARK_DRIVER_MEMORY}"	"${SPARK_DRIVER_MEMORY_OVERHEAD}"	"${LOG4J_CFG}"	"${SPARK_APP_NAME}"
}

function CredsBuild {
	bash "${ROOT_PATH}"/build_creds.sh "${OMEGA_USERNAME}" "${OMEGA_PASSWORD}" "${CH_HOST}" "${CH_PORT}"
}

function GenerateDDLBuild {
	bash "${ROOT_PATH}"/toolbox/build_generate_ddl.sh "${USER}" "${LINUX_LOCAL_PATH_KEYTAB}" "${SPARK_YARN_QUEUE}" "${PYTHON_EXE_PATH}" "${SPARK_INSTANCES}" "${SPARK_EXECUTOR_CORES}" "${SPARK_EXECUTOR_MEMORY}" "${SPARK_EXECUTOR_MEMORY_OVERHEAD}" "${SPARK_DRIVER_CORES}" "${SPARK_DRIVER_MEMORY}" "${SPARK_DRIVER_MEMORY_OVERHEAD}" "${GENERATE_DDL_ARGS_APP}" "${LOG4J_CFG}" "${SPARK_APP_NAME}"
	bash toolbox/generate_ddl.sh
}

function JobBuild {
	bash "${ROOT_PATH}"/jobs/build_job.sh "${JOB_NAME}"
}

function DagBuild {
	bash "${ROOT_PATH}"/dags/build_dag.sh "${HDFS_APP_AIRFLOW_PATH}" "${HDFS_PERSON_PATH}" "${IPA_USERNAME}" "${DAG_ID}" "${START_DATE_DAG}" "${SCHEDULE_INTERVAL}" ${DAGRUN_TIMEOUT} "${TAGS}" "${MAX_ACTIVE_RUNS}" "${CATCHUP}" "${TASK_ID}" "${INCREMENT}" "${SAVE_INTERVAL}" "${START_DATE}" "${END_DATE}" "${JOB_NAME}" "${CH_SCHEMA}" "${CH_DISTRIBUTED_TABLE}" "${CH_MERGETREE_TABLE}" "${HIVE_SCHEMA}" "${HIVE_TABLE}" "${CH_PARTITION_BY_FIELD}" "${CH_CLUSTER}" "${PYTHON_EXE_PATH}" "${SPARK_YARN_QUEUE}" "${SPARK_DRIVER_CORES}" "${SPARK_DRIVER_MEMORY}" "${SPARK_EXECUTOR_CORES}" "${SPARK_EXECUTOR_MEMORY}" "${SPARK_INSTANCES}" "${SPARK_EXECUTOR_MEMORY_OVERHEAD}" "${SPARK_DRIVER_MEMORY_OVERHEAD}"
}

function KeytabLoad {
  set -x
  eval "hdfs dfs -copyFromLocal -f /home/${IPA_USERNAME}/.auth/${IPA_USERNAME}.keytab ${HDFS_PERSON_PATH}/.auth"
  set +x
}

function CredsLoad {
  set -x
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/creds.py ${HDFS_PERSON_PATH}/.auth"
	set +x
}

function JobsLoad {
  set -x
  eval "zip -r jobs.zip jobs"
  eval "hdfs dfs -copyFromLocal -f	${LINUX_PROJECT_AIRFLOW_PATH}/jobs.zip ${HDFS_APP_AIRFLOW_PATH}"
  set +x
}

function ProjectLoadAirflow {
  # eval нужен для правильной интерпретации путей в одинарных кавычках
  set -x
	eval "hdfs dfs -mkdir -p ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -mkdir -p ${HDFS_APP_AIRFLOW_PATH}"
	eval "zip -r src.zip src"
	eval "zip -r jobs.zip jobs"
	eval "hdfs dfs -copyFromLocal -f /home/${IPA_USERNAME}/.auth/${IPA_USERNAME}.keytab ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_AIRFLOW_PATH}/src.zip ${HDFS_APP_AIRFLOW_PATH}"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_AIRFLOW_PATH}/clickhouse-jdbc-0.3.2-patch11-all.jar ${HDFS_APP_AIRFLOW_PATH}"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_AIRFLOW_PATH}/main.py ${HDFS_APP_AIRFLOW_PATH}"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_AIRFLOW_PATH}/creds.py ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -copyFromLocal -f	${LINUX_PROJECT_AIRFLOW_PATH}/jobs.zip ${HDFS_APP_AIRFLOW_PATH}"
	set +x
}

function ProjectLoadOozie {
  set -x
	eval "hdfs dfs -mkdir -p ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -mkdir -p ${HDFS_APP_OOZIE_PATH}"
	eval "hdfs dfs -mkdir -p ${HDFS_APP_OOZIE_PATH}/jobs"
	eval "hdfs dfs -mkdir -p ${HDFS_APP_OOZIE_PATH}/src"
	eval "hdfs dfs -mkdir -p ${HDFS_APP_OOZIE_PATH}/workflow"
	eval "hdfs dfs -copyFromLocal -f /home/${IPA_USERNAME}/.auth/${IPA_USERNAME}.keytab ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/src/* ${HDFS_APP_OOZIE_PATH}/src"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/jobs/* ${HDFS_APP_OOZIE_PATH}/jobs"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/clickhouse-jdbc-0.3.2-patch11-all.jar ${HDFS_APP_OOZIE_PATH}"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/main.py ${HDFS_APP_OOZIE_PATH}"
	eval "hdfs dfs -copyFromLocal -f ${LINUX_PROJECT_OOZIE_PATH}/creds.py ${HDFS_PERSON_PATH}/.auth"
	eval "hdfs dfs -copyFromLocal -f  ${LINUX_PROJECT_OOZIE_PATH}/oozie_start.sh ${HDFS_APP_OOZIE_PATH}"
	eval "hdfs dfs -copyFromLocal -f  ${LINUX_PROJECT_OOZIE_PATH}/workflow/* ${HDFS_APP_OOZIE_PATH}/workflow"
	eval "hdfs dfs -copyFromLocal -f  ${LINUX_PROJECT_OOZIE_PATH}/log4j.properties ${HDFS_APP_OOZIE_PATH}"
	set +x
}