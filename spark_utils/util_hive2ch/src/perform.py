import importlib

from pyspark.sql import DataFrame
from src.dependencies import logger


class Perform:
    def __init__(self,
                 job_name,
                 database,
                 distributed_table,
                 replicated_table,
                 spark_session,
                 config,
                 ch_rest,
                 schema_ch,
                 sharding_key_ch,
                 schema_hive,
                 hive_table,
                 sharding_key_hive,
                 cluster,
                 ):
        self.job_name = job_name
        self.database = database
        self.distributed_table = distributed_table
        self.replicated_table = replicated_table
        self.spark_session = spark_session
        self.config = config
        self.ch_rest = ch_rest
        self.schema_ch = schema_ch
        self.sharding_key_ch = sharding_key_ch
        self.schema_hive = schema_hive
        self.hive_table = hive_table
        self.sharding_key_hive = sharding_key_hive
        self.cluster = cluster

    def etl_task(self,start_date, end_date):

        logger.info(f'Extract data from Hive table \n')
        extract_df = self._extract_data(start_date, end_date)

        logger.info(f'Transform df \n')
        transformed_data = self._transform_data(extract_df)

        logger.info(f'Load data to clickhouse table \n')
        self._load_data(transformed_data)

        return

    def _extract_data(self, start_date, end_date):

        if start_date:
            if end_date:
                main_df = self.spark_session.read \
                    .table(f"{self.schema_hive}.{self.hive_table}") \
                    .filter(f"{self.sharding_key_ch} >= '{start_date}' and {self.sharding_key_ch} <= '{end_date}'")
                return main_df

            main_df = self.spark_session.read \
                .table(f"{self.schema_hive}.{self.hive_table}") \
                .filter(f"{self.sharding_key_ch} >= '{start_date}'")
            return main_df

        main_df = self.spark_session.read \
            .table(f"{self.schema_hive}.{self.hive_table}")
        return main_df

    def _transform_data(self, main_df: DataFrame) -> DataFrame:
        logger.info(f'Transform from jobs.{self.job_name}')
        job_module = importlib.import_module(f'jobs.{self.job_name}')
        transformed_df = job_module.transform_data(main_df=main_df, sc=self.spark_session)
        return transformed_df

    def _load_data(self, df: DataFrame):
        logger.info(f'Loading dataframe to table {self.database}.{self.distributed_table}')
        try:
            df.write \
                .mode('append') \
                .option('dbtable', f'{self.database}.{self.distributed_table}') \
                .option('isolationLevel', 'NONE') \
                .option('batchsize', '100000') \
                .option('numPartitions', '10000') \
                .jdbc(url=self.config.jdbc_url(),
                      table=f'{self.database}.{self.distributed_table}',
                      properties=self.config.properties())
        except ConnectionError as e:
            logger.error('Error happend')
        else:
            logger.info(f'DataFrame was successfully loaded.\n')
            return df.explain()