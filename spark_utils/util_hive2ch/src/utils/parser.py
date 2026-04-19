import argparse
from datetime import datetime


def parse_arguments():
    """Парсинг аргументов командной строки с улучшенной валидацией"""
    parser = argparse.ArgumentParser(
        description='Run hive2ch Job - копирование данных из Hive в ClickHouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Полная загрузка
  python main.py --job_name my_job --increment 0 --schema_ch rrb_ext --distributed_table my_table \\
                 --replicated_table my_table_rrmt --schema_hive default --hive_table source \\
                 --sharding_key_ch dt --cluster sh4_r4

  # Инкремент за последние 7 дней
  python main.py --job_name my_job --increment 1 --save_interval 7 ...

  # Инкремент за период
  python main.py --job_name my_job --increment 2 --start_date 2025-01-01 --end_date 2025-12-31 ...
        """
    )

    # Обязательные параметры
    parser.add_argument(
        '--job_name',
        type=str,
        required=True,
        help='Имя джобы в папке jobs/ (без расширения .py)'
    )

    parser.add_argument(
        '--increment',
        type=int,  # Сразу как int!
        choices=[0, 1, 2, 3],  # Валидация на месте
        required=True,
        help='Режим загрузки: 0-full, 1-инкремент с save_interval, 2-инкремент по диапазону, 3-тест без удаления'
    )

    parser.add_argument(
        '--schema_ch',
        type=str,
        required=True,
        help='Схема в ClickHouse'
    )

    parser.add_argument(
        '--distributed_table',
        type=str,
        required=True,
        help='Имя distributed таблицы в ClickHouse'
    )

    parser.add_argument(
        '--replicated_table',
        type=str,
        required=True,
        help='Имя ReplicatedMergeTree таблицы в ClickHouse'
    )

    parser.add_argument(
        '--schema_hive',
        type=str,
        required=True,
        help='Схема в Hive'
    )

    parser.add_argument(
        '--hive_table',
        type=str,
        required=True,
        help='Имя таблицы в Hive'
    )

    parser.add_argument(
        '--sharding_key_ch',
        type=str,
        required=True,
        help='Ключ шардирования/партицирования в ClickHouse (тип Date)'
    )

    parser.add_argument(
        '--cluster',
        type=str,
        required=True,
        help='Имя кластера ClickHouse'
    )

    # Опциональные параметры
    parser.add_argument(
        '--start_date',
        type=str,
        default=None,
        help='Начальная дата для инкрементальной загрузки (формат: YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end_date',
        type=str,
        default=None,
        help='Конечная дата для инкрементальной загрузки (формат: YYYY-MM-DD)'
    )

    parser.add_argument(
        '--save_interval',
        type=int,
        default=1,
        help='Интервал сохранения в днях для increment=1 (по умолчанию: 1)'
    )

    parser.add_argument(
        '--sharding_key_hive',
        type=str,
        default=None,
        help='Ключ шардирования в Hive (опционально)'
    )

    # Парсим с валидацией
    args = parser.parse_args()

    # Дополнительная валидация
    if args.increment not in [0, 1, 2, 3]:
        raise ValueError(f"Incorrect increment: {args.increment}")
    
    elif args.increment in [2, 3]:
        if not args.start_date:
            parser.error("Для increment=2 или 3 необходимо указать --start_date")

        # Проверка формата даты
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            parser.error(f"start_date '{args.start_date}' имеет неверный формат. Ожидается YYYY-MM-DD")

        if args.end_date:
            try:
                datetime.strptime(args.end_date, '%Y-%m-%d')
            except ValueError:
                parser.error(f"end_date '{args.end_date}' имеет неверный формат. Ожидается YYYY-MM-DD")

    if args.increment == 1 and args.save_interval < 1:
        parser.error("save_interval должен быть >= 1")

    return args