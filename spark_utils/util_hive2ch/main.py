from pyspark.sql import SparkSession
import argparse
import time
from datetime import date, datetime, timedelta

from src.shared import create_spark_session
from src.dependencies import CHConfig, logger, ClickhouseRest
from src.shared import delete_partitions
from src.perform import Perform


def main():
    parser = argparse.ArgumentParser(description='Run hive2ch Job')
    parser.add_argument('--job_name', type=str, dest='job_name', action='store', required=True,
                        help='Job name, that you want to run')
    # parser.add_argument('--month', type=str, action='append', required=False, help='month partition')
    parser.add_argument('--start_date', type=str, action='store', default=None, required=False,
                        help='start date for job')
    parser.add_argument('--end_date', type=str, action='store', default=None, required=False, help='end date for job')
    parser.add_argument('--increment', type=str, action='store', default=None, required=True,
                        help='if =(1 or 2) then increment load, else full load')
    parser.add_argument('--save_interval', type=str, action='store', default=None, required=False, help='save_interval')
    parser.add_argument('--schema_ch', type=str, action='store', default=None, required=True,
                        help='schema_ch')
    parser.add_argument('--distributed_table', type=str, action='store', default=None, required=True,
                        help='distributed_table')
    parser.add_argument('--replicated_table', type=str, action='store', default=None, required=True,
                        help='replicated_table')
    parser.add_argument('--schema_hive', type=str, action='store', default=None, required=True,
                        help='schema_hive')
    parser.add_argument('--hive_table', type=str, action='store', default=None, required=True,
                        help='hive_table')
    parser.add_argument('--sharding_key_ch', type=str, action='store', default=None, required=True,
                        help='sharding_key_ch')
    parser.add_argument('--sharding_key_hive', type=str, action='store', default=None, required=False,
                        help='sharding_key_hive')
    parser.add_argument('--cluster', type=str, action='store', default=None, required=True,
                        help='cluster')
    args = parser.parse_args()

    if int(args.increment) == 3:
        # ТЕСТОВЫЙ ЗАПУСК БЕЗ УДАЛЕНИЯ ПАРТИЦИЙ инкремент по отрезку времени (start_date и end_date включительно)
        if args.start_date:
            start_date_typed: date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            if args.end_date:
                end_date_typed: date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            else:
                end_date_typed: date = datetime.today().date() - timedelta(days=1)
                logger.info(f'''end_date did not exist, but was calculated as t-1: {end_date_typed}''')
        else:
            raise ValueError("start_date or/and end_date doesn't exists")

    elif int(args.increment) == 2:
        # инкремент по отрезку времени (start_date и end_date включительно)
        if args.start_date:
            start_date_typed: date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            if args.end_date:
                end_date_typed: date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            else:
                end_date_typed: date = datetime.today().date() - timedelta(days=1)
                logger.info(f'''end_date did not exist, but was calculated as t-1: {end_date_typed}''')
        else:
            raise ValueError("start_date or/and end_date doesn't exists")

    elif int(args.increment) == 1:
        # increment t-1 with save_interval (save_interval is calculated from the end_date_typed)
        if args.save_interval:
            try:
                save_interval = int(args.save_interval)
                if save_interval < 1:
                    raise ValueError("Incorrect value <save_interval>, acceptable values save_interval >= 1")
            except:
                raise ValueError("Incorrect value <save_interval>, acceptable values save_interval >= 1")
        else:
            save_interval = 1

        end_date_typed: date = datetime.today().date() - timedelta(days=1)
        start_date_typed: date = end_date_typed - timedelta(days=save_interval)


    elif int(args.increment) == 0:
        # Full load
        start_date_typed = None
        end_date_typed = None

    else:
        raise ValueError("Incorrect value <increment>, acceptable values [0,1,2,3]")

    job_name = args.job_name
    schema_ch = args.schema_ch
    distributed_table = args.distributed_table
    replicated_table = args.replicated_table
    database = args.schema_ch
    config = CHConfig()
    sharding_key_ch = args.sharding_key_ch
    schema_hive = args.schema_hive
    hive_table = args.hive_table
    sharding_key_hive = args.sharding_key_hive
    cluster = args.cluster
    ch_rest = ClickhouseRest(config)

    try:
        logger.info(f'Starting Spark application: {schema_ch}.{distributed_table}')
        spark_session: SparkSession = create_spark_session(f'{schema_ch}.{distributed_table}')

        if job_name is not None:
            if int(args.increment) == 3:
                logger.info(f'''Test without deleting partitions and truncate table \n''')

            elif int(args.increment) in [1, 2]:
                delete_partitions(ch_rest=ch_rest,
                                  cluster=cluster,
                                  database=database,
                                  replicated_table=replicated_table,
                                  start_date=start_date_typed,
                                  end_date=end_date_typed,
                                  sharding_key_ch=sharding_key_ch)
            elif int(args.increment) == 0:
                logger.info(f'''Truncate table {database}.{replicated_table} on cluster {cluster} \n''')
                ch_rest.query(f"truncate table {database}.{replicated_table} on cluster {cluster}")

            logger.info(f'Execute job: {job_name}')
            perform = Perform(
                job_name=job_name,
                database=database,
                distributed_table=distributed_table,
                replicated_table=replicated_table,
                spark_session=spark_session,
                config=config,
                ch_rest=ch_rest,
                schema_ch=schema_ch,
                sharding_key_ch=sharding_key_ch,
                schema_hive=schema_hive,
                hive_table=hive_table,
                sharding_key_hive=sharding_key_hive,
                cluster=cluster)
            perform.etl_task(start_date_typed, end_date_typed)

        else:
            raise ValueError("Incorrect value <job_name>, acceptable values []")

    except Exception as e:
        logger.error(f'Exception is: {e} ')
        raise e
    else:
        logger.info(f'Shutdown Spark application: {schema_ch}.{distributed_table}')
        # spark_session.stop()
    return None


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    logger.info(f"Spark Application has finished. Time spent: {end - start}")