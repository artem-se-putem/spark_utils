from datetime import datetime, timedelta

from se.airflow import DAG
from se.airflow.utils.dates import days_ago
from se.airflow.operators.empty import EmptyOperator
from se.airflow.providers.spark import SparkSqlOperatorSE, SparkSubmitOperatorSE

args = {
    "owner": "22343006_omega-sbrf-ru" # обязательно явно, ipa_username
}

hdfs_app_path = "hdfs://arnsdpldbr2/user/team/team_transformation_analytical/oozie-app/hive2ch"
hdfs_person_path = f"hdfs://arnsdpldbr2/user/{args['owner']}"

with DAG(
        dag_id="wf_rdwh_hive_to_ch__template_dag", # обязательный, под этим названием будет отображаться даг в airflow ui
        default_args=args, # словарь параметров,которые применяются для каждого оператора в даге
        start_date=days_ago(1), # обязательный, дата старта при запуске дага, поддерживает datetime
        schedule_interval=None, # рассписание автозапуска дага, указывать в формате cron-выражения
        dagrun_timeout=timedelta(minutes=60), # timeout dag run
        tags=["rdwh", args['owner'], "template_dag"], # ставим rdwh, login_omega и любые другие
        max_active_runs=1, # максимальное число активных dag run
        catchup=False, # при старте дага, запускать ли предыдущие НЕуспешные dag runs, начиная от start_date (включительно) до даты запуска дага (невключительно)
) as dag:

    spark_job = SparkSubmitOperatorSE(
        task_id="load_hive_to_ch_t_cred_portf_metric_mon_appl_tmp2_load_test",
        application=f"{hdfs_app_path}/main.py", # запускается на драйвере spark приложения
        application_args=[
            "--job_name", "t_cred_portf_metric_mon_appl_tmp2_load_test", # имя джобы в папке src/jobs
            "--increment", "0", # 0 - full, 1 - increment с save_interval, 2 - от start_date до end_date
            "--schema_ch", "rrb_ext", # схема таблиц в кликхаусе
            "--distributed_table", "t_cred_portf_metric_mon_appl_tmp2_load_test", # имя шардированной таблицы
            "--replicated_table", "t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test", # имя таблицы, над которой построена шардированная
            "--schema_hive", "t_team_cpu_reporting_rrm", # схема таблицы в хайве, из которой грузим
            "--hive_table", "t_cred_portf_metric_mon_appl_tmp2", # имя таблицы в хайве, из которой грузим
            "--sharding_key_ch", "gregor_dt", # ключ шардированая в кликхаусе, для удаления по партициям (только одно поле, тип=date)
            "--cluster", "sh8_r1" # имя кластера в кликхаусе
        ], # аргументы application
        py_files=f"{hdfs_person_path}/.auth/creds.py,{hdfs_person_path}/.auth/{args['owner']}.keytab,{hdfs_app_path}/src.zip,{hdfs_app_path}/jobs.zip", # здесь указываем все необходимые файлы (.creds.py и keytab) и zip архив приложения и zip архив с jobs
        jars=f"{hdfs_app_path}/clickhouse-jdbc-0.3.2-patch11-all.jar", # jar файлы кладем сюда, и указываем их в conf, параметр jars
        conf={"spark.master": 'yarn', # обязательный
              "spark.submit.deployMode": 'cluster', # обязательный
              "spark.yarn.queue": 'team_rdwh_reserv', # обязательный
              "spark.hadoop.hive.metastore.uris": 'thrift://pplas-ldbr00365.sdpdi.df.sbrf.ru:9083,thrift://pplas-ldbr00366.sdpdi.df.sbrf.ru:9083,thrift://pplas-ldbr00409.sdpdi.df.sbrf.ru:9083,thrift://pplas-ldbr00410.sdpdi.df.sbrf.ru:9083,thrift://pplos-ldbr00323.sdpdi.df.sbrf.ru:9083,thrift://pplos-ldbr00423.sdpdi.df.sbrf.ru:9083,thrift://pplos-ldbr00443.sdpdi.df.sbrf.ru:9083', # обязательный, это адрес MetaStore Hive, без этого параметра не будет видеть MetaStore Hive
              "spark.pyspark.python": '/opt/sdp/mlpy3811v23/bin/python', # обязательный, путь к интерпретатору питона для executor, иначе будут ошибки с импортами сторонних библиотек
              "spark.pyspark.driver.python": '/opt/sdp/mlpy3811v23/bin/python', # обязательный, путь к интерпретатору питона для driver, иначе будут ошибки с импортами сторонних библиотек
              "spark.driver.cores": 8, # кол-во ядер driver, не менять
              "spark.driver.memory": "20g", # оператива driver, не менять
              "spark.executor.cores": 10, # кол-во ядер на executor, делаешь больше, если нужна выше скорость (вместе с ним менять и параметры spark.default.parallelism и spark.sql.shuffle.partitions)
              "spark.executor.memory": "20g", # оператива на executor
              "spark.executor.instances": 10, # кол-во executor (executor.cores*executor.instances должно быть меньше 40, для дага на постоянной основе, чтобы не мешать другим коллегам, грузить данные, тк общее кол-во ядер на очереди team_rdwh_reserv =1200)
              "spark.executor.memoryOverhead": "4g", # 20% от executor.memory
              "spark.driver.memoryOverhead": "4g", # 20% от driver.memory
              "spark.sql.parquet.binaryAsString": "true", # читать тип binary как string
              "spark.sql.hive.convertMetastoreParquet": "false", # для совместимости типов данных spark и старых версий hive (иначе timestamp поменяет формат)
              "spark.shuffle.service.enabled": "true", # всегда должен true, для шафл операций между executors
              "spark.sql.parquet.readLegacyFormat": "true", # для совместимости с legacy версиями parquet
              "spark.hadoop.validateOutputSpecs": "false", # чтобы не было ошибок, если перезаписываешь какие то данные
              "spark.hadoop.mapred.input.dir.recursive": "true", # для чтения партицированных таблиц всегда делаем true
              "spark.sql.catalogImplementation": "hive", # указывать, если работаешь с hive
              "spark.hadoop.metastore.skip.load.functions.on.init": "true", # true, если НЕ используешь udf из hive (то есть всегда true)
              "jars": "clickhouse-jdbc-0.3.2-patch11-all.jar", # jar для работы с clickhouse
              "spark.yarn.access.hadoopFileSystems": 'hdfs://hdfsgw', # ДЛЯ ЧТЕНИЯ ИЗ ПОДПИСКИ: без этого параметра будет ошибка кербероса
              "spark.hadoop.yarn.timeline-service.enabled": "false"
              },
        verbose=True # True - вывод подробных логов
    )

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    start >> [spark_job, ] >> finish