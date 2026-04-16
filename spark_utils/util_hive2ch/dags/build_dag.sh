#!/bin/bash

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

export hdfs_app_path=${1}
export hdfs_person_path=${2}
export owner=${3}
export dag_id=${4}
export start_date_dag=${5}
export schedule_interval=${6}
export dagrun_timeout=${7}
export tags=${8}
export max_active_runs=${9}
export catchup=${10}
export task_id=${11}
export increment=${12}
export save_interval=${13}
export start_date=${14}
export end_date=${15}
export job_name=${16}
export schema_ch=${17}
export distributed_table=${18}
export replicated_table=${19}
export schema_hive=${20}
export hive_table=${21}
export sharding_key_ch=${22}
export cluster=${23}
export python_exe_path=${24}
export spark_yarn_queue=${25}
export spark_driver_cores=${26}
export spark_driver_memory=${27}
export spark_executor_cores=${28}
export spark_executor_memory=${29}
export spark_instances=${30}
export spark_executor_memory_overhead=${31}
export spark_driver_memory_overhead=${32}

echo "Заполняем шаблон ${SCRIPTPATH}/dag_template.py.template"

${PYTHON_EXE_PATH} ${SCRIPTPATH}/generate_dag.py \
--hdfs_app_path "${hdfs_app_path}" \
--hdfs_person_path "${hdfs_person_path}" \
--owner "${owner}" \
--dag_id "${dag_id}" \
--start_date_dag "${start_date_dag}" \
--schedule_interval "${schedule_interval}" \
--dagrun_timeout "${dagrun_timeout}" \
--tags "${tags}" \
--max_active_runs "${max_active_runs}" \
--catchup "${catchup}" \
--task_id "${task_id}" \
--increment "${increment}" \
--save_interval "${save_interval}" \
--start_date "${start_date}" \
--end_date "${end_date}" \
--job_name "${job_name}" \
--schema_ch "${schema_ch}" \
--distributed_table "${distributed_table}" \
--replicated_table "${replicated_table}" \
--schema_hive "${schema_hive}" \
--hive_table "${hive_table}" \
--sharding_key_ch "${sharding_key_ch}" \
--cluster "${cluster}" \
--python_exe_path "${python_exe_path}" \
--spark_yarn_queue "${spark_yarn_queue}" \
--spark_driver_cores "${spark_driver_cores}" \
--spark_driver_memory "${spark_driver_memory}" \
--spark_executor_cores "${spark_executor_cores}" \
--spark_executor_memory "${spark_executor_memory}" \
--spark_instances "${spark_instances}" \
--spark_executor_memory_overhead "${spark_executor_memory_overhead}" \
--spark_driver_memory_overhead "${spark_driver_memory_overhead}"