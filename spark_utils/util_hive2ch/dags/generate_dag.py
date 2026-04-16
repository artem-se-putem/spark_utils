import argparse
import sys
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined

sys.path.append(str(Path(__file__).parent.parent))

from src.dependencies import logger

def main():
    parser = argparse.ArgumentParser(description='Run T-1 HDFS2CH_RISKZAPROS parsed risk-request Job')
    parser.add_argument('--hdfs_app_path', type=str, action='store', default=None, required=True, help='hdfs_app_path')
    parser.add_argument('--hdfs_person_path', type=str, action='store', default=None, required=True, help='hdfs_person_path')
    parser.add_argument('--owner', type=str, action='store', default=None, required=True, help='owner')
    parser.add_argument('--dag_id', type=str, action='store', default=None, required=True, help='dag_id')
    parser.add_argument('--start_date_dag', type=str, action='store', default=None, required=True, help='start_date_dag')
    parser.add_argument('--schedule_interval', type=str, action='store', default=None, required=True, help='schedule_interval')
    parser.add_argument('--dagrun_timeout', type=str, action='store', default=None, required=True, help='dagrun_timeout')
    parser.add_argument('--tags', type=str, action='store', default=None, required=True, help='tags')
    parser.add_argument('--max_active_runs', type=str, action='store', default=None, required=True, help='max_active_runs')
    parser.add_argument('--catchup', type=str, action='store', default=None, required=True, help='catchup')
    parser.add_argument('--task_id', type=str, action='store', default=None, required=True, help='task_id')
    parser.add_argument('--increment', type=str, action='store', default=None, required=True, help='increment')
    parser.add_argument('--save_interval', type=str, action='store', default=None, required=True, help='save_interval')
    parser.add_argument('--start_date', type=str, action='store', default=None, required=True, help='start_date')
    parser.add_argument('--end_date', type=str, action='store', default=None, required=True, help='end_date')
    parser.add_argument('--job_name', type=str, action='store', default=None, required=True, help='job_name')
    parser.add_argument('--schema_ch', type=str, action='store', default=None, required=True, help='schema_ch')
    parser.add_argument('--distributed_table', type=str, action='store', default=None, required=True, help='distributed_table')
    parser.add_argument('--replicated_table', type=str, action='store', default=None, required=True, help='replicated_table')
    parser.add_argument('--schema_hive', type=str, action='store', default=None, required=True, help='schema_hive')
    parser.add_argument('--hive_table', type=str, action='store', default=None, required=True, help='hive_table')
    parser.add_argument('--sharding_key_ch', type=str, action='store', default=None, required=True, help='sharding_key_ch')
    parser.add_argument('--cluster', type=str, action='store', default=None, required=True, help='cluster')
    parser.add_argument('--python_exe_path', type=str, action='store', default=None, required=True, help='python_exe_path')
    parser.add_argument('--spark_yarn_queue', type=str, action='store', default=None, required=True, help='spark_yarn_queue')
    parser.add_argument('--spark_driver_cores', type=str, action='store', default=None, required=True, help='spark_driver_cores')
    parser.add_argument('--spark_driver_memory', type=str, action='store', default=None, required=True, help='spark_driver_memory')
    parser.add_argument('--spark_executor_cores', type=str, action='store', default=None, required=True, help='spark_executor_cores')
    parser.add_argument('--spark_executor_memory', type=str, action='store', default=None, required=True, help='spark_executor_memory')
    parser.add_argument('--spark_instances', type=str, action='store', default=None, required=True, help='spark_instances')
    parser.add_argument('--spark_executor_memory_overhead', type=str, action='store', default=None, required=True, help='spark_executor_memory_overhead')
    parser.add_argument('--spark_driver_memory_overhead', type=str, action='store', default=None, required=True, help='spark_driver_memory_overhead')
    args = parser.parse_args()

    logger.info(f'Starting Spark application: generating dag dags/{args.distributed_table}.py')

    try:
        generate_dag(
            hdfs_app_path=args.hdfs_app_path,
            hdfs_person_path=args.hdfs_person_path,
            owner=args.owner,
            dag_id=args.dag_id,
            start_date_dag=args.start_date_dag,
            schedule_interval=args.schedule_interval,
            dagrun_timeout=args.dagrun_timeout,
            tags=args.tags,
            max_active_runs=args.max_active_runs,
            catchup=args.catchup,
            task_id=args.task_id,
            increment=args.increment,
            save_interval=args.save_interval,
            start_date=args.start_date,
            end_date=args.end_date,
            job_name=args.job_name,
            schema_ch=args.schema_ch,
            distributed_table=args.distributed_table,
            replicated_table=args.replicated_table,
            schema_hive=args.schema_hive,
            hive_table=args.hive_table,
            sharding_key_ch=args.sharding_key_ch,
            cluster=args.cluster,
            python_exe_path=args.python_exe_path,
            spark_yarn_queue=args.spark_yarn_queue,
            spark_driver_cores=args.spark_driver_cores,
            spark_driver_memory=args.spark_driver_memory,
            spark_executor_cores=args.spark_executor_cores,
            spark_executor_memory=args.spark_executor_memory,
            spark_instances=args.spark_instances,
            spark_executor_memory_overhead=args.spark_executor_memory_overhead,
            spark_driver_memory_overhead=args.spark_driver_memory_overhead
        )

    except Exception as e:
        logger.error(f'Exception is: {e} ')
        logger.info(f'Генерация дага {args.schema_ch}.{args.distributed_table} не выполнилась!')

    return None

def generate_dag(hdfs_app_path, hdfs_person_path, owner, dag_id, start_date_dag, schedule_interval,
                 dagrun_timeout, tags, max_active_runs, catchup, task_id, increment, save_interval,
                 start_date, end_date, job_name, schema_ch, distributed_table, replicated_table,
                 schema_hive, hive_table, sharding_key_ch, cluster, python_exe_path, spark_yarn_queue, spark_driver_cores,
                 spark_driver_memory, spark_executor_cores, spark_executor_memory,
                 spark_instances,spark_executor_memory_overhead, spark_driver_memory_overhead):
    # Формируем контекст для jinja2 для dag_template.py.template clickhouse
    logger.info(f'Generating dag {schema_ch}.{distributed_table}')
    context = {
        "hdfs_app_path": hdfs_app_path,
        "hdfs_person_path": hdfs_person_path,
        "owner": owner,
        "dag_id": dag_id,
        "start_date_dag": start_date_dag,
        "schedule_interval": schedule_interval,
        "dagrun_timeout": dagrun_timeout,
        "tags": tags,
        "max_active_runs": max_active_runs,
        "catchup": catchup,
        "task_id": task_id,
        "increment": increment,
        "save_interval": save_interval,
        "start_date": start_date,
        "end_date": end_date,
        "job_name": job_name,
        "schema_ch": schema_ch,
        "distributed_table": distributed_table,
        "replicated_table": replicated_table,
        "schema_hive": schema_hive,
        "hive_table": hive_table,
        "sharding_key_ch": sharding_key_ch,
        "cluster": cluster,
        "python_exe_path": python_exe_path,
        "spark_yarn_queue": spark_yarn_queue,
        "spark_driver_cores": spark_driver_cores,
        "spark_driver_memory": spark_driver_memory,
        "spark_executor_cores": spark_executor_cores,
        "spark_executor_memory": spark_executor_memory,
        "spark_instances": spark_instances,
        "spark_executor_memory_overhead": spark_executor_memory_overhead,
        "spark_driver_memory_overhead": spark_driver_memory_overhead
    }

    print(context)

    # Загружаем шаблон
    env = Environment(loader=FileSystemLoader('./dags/'), undefined=StrictUndefined)

    # Рендерим и сохраняем
    logger.info(f'Trying render ddl_template dags/dag_template.py.template')
    template = env.get_template('dag_template.py.template')
    output = template.render(**context)

    with open(f'dags/{context["distributed_table"]}.py', 'w', encoding='utf-8') as f:
        f.write(output)
    logger.info(f'Downloaded dag dags/{context["distributed_table"]}.py')
    logger.info(output)
    logger.info(f'You could copy dag from dags/{context["distributed_table"]}.py or from console')
    logger.info(f'Bash-operation helps you: cat dags/{distributed_table}.py | iconv -f windows-1251 -t utf-8')

    return

main()