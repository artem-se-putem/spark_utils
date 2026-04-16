

#!/bin/bash



START_DTTM=`date '+%Y%m%d%H%M'`

export PYTHONPATH=":src"



/usr/sdp/current/spark3-client/bin/spark-submit \

--name "hive2ch.{START_DTTM}" \

--master yarn \

--principal artem \

--keytab ~/.auth/artem.keytab \

--deploy-mode client \

--queue team_rdwh_reserv \

--conf spark.pyspark.driver.python="/opt/sdp/mlpy3811v23/bin/python" \

--conf spark.pyspark.python="/opt/sdp/mlpy3811v23/bin/python" \

--conf spark.driver.cores=8 \

--conf spark.driver.memory=20g \

--conf spark.executor.cores=2 \

--conf spark.executor.memory=40g \

--conf spark.executor.instances=5 \

--conf spark.executor.memoryOverhead=4g \

--conf spark.driver.memoryOverhead=4g \

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

toolbox/generate_ddl.py \

--schema_ch rrb_ext \

--distributed_table stg_t_cred_portf_metric_mon \

--replicated_table stg_t_cred_portf_metric_mon_rrmt \

--schema_hive arnsdprisk__custom_risk_np_reporting \

--hive_table t_cred_portf_metric_mon \

--sharding_key_ch gregor_dt \

--cluster sh4_r4
