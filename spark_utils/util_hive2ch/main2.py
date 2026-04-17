import time
from utils.parser import parse_arguments


def main():
    args = parse_arguments()
    increment = int(args.increment)
    spark_session = None

    try:
        # Валидация
        if increment not in [0, 1, 2, 3]:
            raise ValueError(f"Incorrect increment: {increment}")

        # Расчет дат
        start_date_typed, end_date_typed = calculate_dates(args, increment)

        # Создание сессии
        spark_session = create_spark_session(f'{args.schema_ch}.{args.distributed_table}')
        ch_rest = ClickhouseRest(CHConfig())

        # Очистка партиций
        if increment in [1, 2]:
            delete_partitions(...)
        elif increment == 0:
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