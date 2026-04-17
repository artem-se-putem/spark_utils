import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def setup_logger(
        name: Optional[str] = None,
        level: int = logging.INFO,
        log_file: Optional[str] = None
) -> logging.Logger:
    """
    Настройка логгера с поддержкой файлов

    Args:
        name: имя логгера
        level: уровень логирования
        log_file: путь к файлу лога (опционально)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        logger.handlers.clear()

    logger.setLevel(level)

    # Форматтер
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (если указан)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger

# Настройка уровня логирования из переменной окружения
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level, logging.INFO)

# Создаем основные логгеры
logger = setup_logger('hive2ch', level=log_level)
logger_debug = setup_logger('hive2ch.debug', level=logging.DEBUG)