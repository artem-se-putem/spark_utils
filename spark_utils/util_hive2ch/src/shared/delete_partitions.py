from datetime import timedelta, date
from src.dependencies import logger

def delete_partitions(ch_rest, cluster: str, database: str, replicated_table: str,
                      start_date: date, end_date: date) -> int:
    """
    Удаляет партиции в ClickHouse по диапазону дат
    
    Args:
        ch_rest: ClickHouse REST клиент
        cluster: имя кластера
        database: имя базы данных
        replicated_table: имя таблицы
        start_date: начальная дата
        end_date: конечная дата
        sharding_key_ch: ключ партицирования (не используется для DROP PARTITION)
    
    Returns:
        int: количество удаленных партиций
    """
    
    logger.info(f"Starting partition deletion for {database}.{replicated_table}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Cluster: {cluster}")
    
    # Получаем список существующих партиций
    logger.debug("Fetching existing partitions from system.parts")
    get_partitions_sql = f"""
    SELECT DISTINCT partition
    FROM system.parts
    WHERE database = '{database}'
      AND table = '{replicated_table}'
      AND active = 1
    """
    
    try:
        existing_partitions = ch_rest.query(get_partitions_sql)
        logger.debug(f"Found existing partitions: {existing_partitions}")
    except Exception as e:
        logger.error(f"Failed to fetch existing partitions: {e}")
        raise
    
    # Генерируем и перебираем даты
    days_count = (end_date - start_date).days + 1
    logger.info(f"Total days to process: {days_count}")
    
    deleted_count = 0
    
    for i in range(days_count):
        current_date = start_date + timedelta(days=i)
        partition_value = current_date.strftime('%Y-%m-%d')
        
        logger.debug(f"Checking partition for date: {partition_value}")
        
        if existing_partitions and partition_value in existing_partitions:
            sql = f"""
            ALTER TABLE {database}.{replicated_table} ON CLUSTER {cluster}
            DROP PARTITION '{partition_value}'
            """
            
            try:
                logger.info(f"Dropping partition: {partition_value}")
                ch_rest.query(sql)
                deleted_count += 1
                logger.debug(f"Successfully dropped partition: {partition_value}")
            except Exception as e:
                logger.error(f"Failed to drop partition {partition_value}: {e}")
                raise
        else:
            logger.debug(f"Partition {partition_value} does not exist, skipping")
    
    logger.info(f"Partition deletion completed. Deleted {deleted_count} partitions")
    return deleted_count