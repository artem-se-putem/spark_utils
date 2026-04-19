# src/rest_config.py

import requests
import json
from typing import Optional, Dict, Any, List
from urllib.parse import quote # экранирует специальные символы в SQL
from src.dependencies import logger
from src.dependencies import ClickHouseConfig


class ClickhouseRest:
    """REST клиент для ClickHouse"""
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.base_url = config.http_url()
        self.session = requests.Session()
        
        # Настройка сессии
        self.session.auth = (config.username, config.password)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        # Отключаем проверку SSL для тестов (в продакшене нужно настроить)
        if not config.secure:
            self.session.verify = False
    
    def query(self, sql: str, format: str = "JSON") -> Any:
        """
        Выполняет SQL запрос и возвращает результат
        
        Args:
            sql: SQL запрос
            format: Формат вывода (JSON, CSV, TabSeparated и т.д.)
        
        Returns:
            Результат запроса в виде JSON
        """
        url = f"{self.base_url}/?query={quote(sql)}&default_format={format}"
        
        try:
            logger.debug(f"Executing ClickHouse query: {sql[:200]}...")
            response = self.session.post(url)
            response.raise_for_status()
            
            if format == "JSON":
                return response.json()
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ClickHouse query failed: {e}")
            logger.error(f"SQL: {sql}")
            raise
    
    def query_dataframe(self, sql: str) -> List[Dict]:
        """Выполняет запрос и возвращает результат в виде списка словарей"""
        result = self.query(sql, format="JSON")
        return result.get("data", [])
    
    def execute(self, sql: str) -> bool:
        """
        Выполняет SQL запрос без возврата результата (INSERT, ALTER, CREATE и т.д.)
        
        Returns:
            True если успешно, иначе False
        """
        try:
            self.query(sql, format="JSON")
            logger.debug(f"Query executed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return False
    
    def create_table(self, database: str, table: str, ddl: str, cluster: Optional[str] = None) -> bool:
        """Создает таблицу в ClickHouse"""
        sql = f"CREATE TABLE IF NOT EXISTS {database}.{table} ON CLUSTER {cluster} {ddl}"
        return self.execute(sql)
    
    def drop_table(self, database: str, table: str, cluster: Optional[str] = None) -> bool:
        """Удаляет таблицу"""
        if cluster:
            sql = f"DROP TABLE IF EXISTS {database}.{table} ON CLUSTER {cluster}"
        else:
            sql = f"TRUNCATE TABLE {database}.{table}"
        return self.execute(sql)
    
    def truncate_table(self, database: str, table: str, cluster: Optional[str] = None) -> bool:
        """Очищает таблицу"""
        if cluster:
            sql = f"TRUNCATE TABLE {database}.{table} ON CLUSTER {cluster}"
        else:
            sql = f"TRUNCATE TABLE {database}.{table}"
        return self.execute(sql)
    
    def drop_partition(self, database: str, table: str, partition: str, cluster: Optional[str] = None) -> bool:
        """Удаляет партицию"""
        if cluster:
            sql = f"ALTER TABLE {database}.{table} ON CLUSTER {cluster} DROP PARTITION '{partition}'"
        else:
            sql = f"ALTER TABLE {database}.{table} DROP PARTITION '{partition}'"
        return self.execute(sql)
    
    def get_partitions(self, database: str, table: str) -> List[str]:
        """Получает список партиций таблицы"""
        sql = f"""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = '{database}'
              AND table = '{table}'
              AND active = 1
            ORDER BY partition
        """
        result = self.query_dataframe(sql)
        return [row["partition"] for row in result]
    
    def table_exists(self, database: str, table: str) -> bool:
        """Проверяет существование таблицы"""
        sql = f"""
            SELECT 1
            FROM system.tables
            WHERE database = '{database}'
              AND name = '{table}'
        """
        result = self.query_dataframe(sql)
        return len(result) > 0
    
    def get_table_schema(self, database: str, table: str) -> List[Dict]:
        """Получает схему таблицы"""
        sql = f"DESCRIBE TABLE {database}.{table}"
        return self.query_dataframe(sql)
    
    def insert_dataframe(self, database: str, table: str, df, batch_size: int = 100000):
        """
        Вставляет DataFrame в ClickHouse
        
        Args:
            database: имя БД
            table: имя таблицы
            df: Spark DataFrame
            batch_size: размер батча
        """
        logger.info(f"Inserting data into {database}.{table}")
        
        df.write \
            .mode('append') \
            .option('dbtable', f'{database}.{table}') \
            .option('isolationLevel', 'NONE') \
            .option('batchsize', str(batch_size)) \
            .option('numPartitions', '100') \
            .jdbc(
                url=self.config.jdbc_url(),
                table=f'{database}.{table}',
                properties=self.config.properties()
            )
        
        logger.info("Data inserted successfully")
    
    def close(self):
        """Закрывает сессию"""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()