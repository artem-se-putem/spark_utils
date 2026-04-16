#!/bin/bash

START_DTTM=`date '+%Y%m%d%H%M'`
export PYTHONPATH=":src"

# Для oozie: --keytab 22343006_omega-sbrf-ru.keytab \
# Для локального запуска: --keytab ~/.auth/22343006_omega-sbrf-ru.keytab \

/usr/sdp/current/spark3-client/bin/spark-submit \
--name "hive2ch.{START_DTTM}" \
--master yarn \
--principal 22343006_omega-sbrf-ru \
--keytab ~/.auth/22343006_omega-sbrf-ru.keytab \
--deploy-mode client \
--queue team_rdwh_reserv \
--conf spark.pyspark.driver.python="/opt/sdp/mlpy3811v23/bin/python" \
--conf spark.pyspark.python="/opt/sdp/mlpy3811v23/bin/python" \
--conf spark.driver.cores=4 \
--conf spark.driver.memory=20g \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=16g \
--conf spark.executor.instances=10 \
--conf spark.executor.memoryOverhead=4g \
--conf spark.driver.memoryOverhead=5g \
--conf spark.sql.parquet.binaryAsString=true \
--conf spark.sql.hive.convertMetastoreParquet=false \
--conf spark.shuffle.service.enabled=true \
--conf spark.sql.parquet.readLegacyFormat=true \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.hadoop.mapred.input.dir.recursive=true \
--conf spark.sql.catalogImplementation=hive \
--conf spark.hadoop.metastore.skip.load.functions.on.init=true \
--driver-java-options "-Dlog4j.configuration=file:log4j.properties" \
--jars clickhouse-jdbc-0.3.2-patch11-all.jar \
--py-files src \
main.py \
--job_name arnsdprisk__custom_risk_np_reporting_t_report_demand_v2 \
--increment 1 \
--save_interval 64 \
--schema_ch rrb_ext \
--distributed_table stg_t_report_demand_v2 \
--replicated_table stg_t_report_demand_rmt_v2 \
--schema_hive arnsdprisk__custom_risk_np_reporting \
--hive_table t_report_demand \
--sharding_key_ch ctl_valid_period \
--cluster sh4_r4

# Для справки:
## Для oozie:
# "$@"

## Для local:
#--job_name t_cred_portf_metric_mon_appl_tmp2_load_test \
#--increment 0 \
#--schema_ch rrb_ext \
#--distributed_table t_cred_portf_metric_mon_appl_tmp2_load_test \
#--replicated_table t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test \
#--schema_hive t_team_cpu_reporting_rrm \
#--hive_table t_cred_portf_metric_mon_appl_tmp2 \
#--sharding_key_ch gregor_dt \
#--cluster sh8_r1

#--increment 1
#--save_interval 10

#--increment 2
#--start_date 2025-01-01
# --end_date 2025-02-01

#--increment 3 (без дропа партиций)
#--start_date 2025-01-01
# --end_date 2025-02-01