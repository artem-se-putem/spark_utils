from typing import Optional, Tuple
from datetime import date, datetime, timedelta
from src.dependencies import logger

def get_dates_for_incremental(args, increment: int) -> Tuple[date, date]:
    """
    Расчет дат для загрузки данных

    Args:
        args: аргументы командной строки
        increment: режим загрузки (0, 1, 2, 3)

    Returns:
        Tuple[Optional[date], Optional[date]]: (start_date, end_date)
        Для полной загрузки (increment=0) возвращает (None, None)
    """

    if increment == 1:
        # Инкремент с save_interval
        end_date_typed: date = datetime.today().date() - timedelta(days=1)
        start_date_typed: date = end_date_typed - timedelta(days=args.save_interval)
        logger.info(f"Инкрементальная загрузка за период: {start_date_typed} - {end_date_typed}")
        return start_date_typed, end_date_typed

    elif increment in [2, 3]:
        # Инкремент по диапазону дат
        if not args.start_date:
            raise ValueError("Для increment=2 или 3 необходимо указать --start_date")

        start_date_typed: date = datetime.strptime(args.start_date, '%Y-%m-%d').date()

        if args.end_date:
            end_date_typed: date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date_typed: date = datetime.today().date() - timedelta(days=1)
            logger.info(f"end_date не указан, рассчитан как t-1: {end_date_typed}")

        # Валидация диапазона
        if start_date_typed > end_date_typed:
            raise ValueError(f"start_date {start_date_typed} > end_date {end_date_typed}")

        logger.info(f"Загрузка за диапазон: {start_date_typed} - {end_date_typed}")
        return start_date_typed, end_date_typed

    else:
        raise ValueError(f"Неизвестный режим инкремента: {increment}")