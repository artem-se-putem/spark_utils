import time
from src.utils import parse_arguments, get_dates_for_incremental
from src.dependencies import logger, ClickHouseConfig, ClickhouseRest
from src.shared import create_spark_session, delete_partitions


def main():
    args = parse_arguments()
    increment = int(args.increment)
    spark_session = None

    try:
        # Создание сессии
        spark_session = create_spark_session(f'{args.schema_ch}.{args.distributed_table}')
        ch_rest = ClickhouseRest(ClickHouseConfig.from_env())

        # Очистка партиций
        if increment in [1, 2]:
            # Расчет дат
            start_date_typed, end_date_typed = get_dates_for_incremental(args, increment)

            # Дроп партиций по отрезку дат (включает оба конца отрезка дат)
            delete_partitions(
                ch_rest=ch_rest,
                cluster=args.cluster,
                database=args.schema_ch,
                replicated_table=args.replicated_table,
                start_date=start_date_typed,
                end_date=end_date_typed
            )
        elif increment == 0:
            logger.info("Режим полной загрузки (без фильтрации по датам)")
            ch_rest.query(f"truncate table {args.schema_ch}.{args.replicated_table} on cluster {args.cluster}")
        # increment == 3 - пропускаем очистку

        # Выполнение
        perform = Perform(...)
        perform.etl_task(start_date_typed, end_date_typed)

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    logger.info(f"Spark Application has finished. Time spent: {end - start}")