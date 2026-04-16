from pyspark.sql import SparkSession
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.shared import create_spark_session
from src.dependencies import logger

from jinja2 import Environment, FileSystemLoader, StrictUndefined

def main():
    parser = argparse.ArgumentParser(description='Run T-1 HDFS2CH_RISKZAPROS parsed risk-request Job')
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

    schema_ch = args.schema_ch
    distributed_table = args.distributed_table
    replicated_table = args.replicated_table
    sharding_key_ch = args.sharding_key_ch
    schema_hive = args.schema_hive
    hive_table = args.hive_table
    cluster = args.cluster

    logger.info(f'Starting Spark application: {schema_ch}.{distributed_table}')
    spark_session: SparkSession = create_spark_session(f'{schema_ch}.{distributed_table}')

    try:
        generate_ddl(sc=spark_session,
                     schema_ch=schema_ch,
                     distributed_table=distributed_table,
                     replicated_table=replicated_table,
                     sharding_key_ch=sharding_key_ch,
                     schema_hive=schema_hive,
                     hive_table=hive_table,
                     cluster=cluster)

    except Exception as e:
        logger.error(f'Exception is: {e} ')
    else:
        logger.info(f'Shutdown Spark application: {schema_ch}.{distributed_table}')
        spark_session.stop()
    return None

def generate_ddl(sc, schema_ch, distributed_table, replicated_table, sharding_key_ch, schema_hive, hive_table, cluster):
    # Формируем контекст для jinja2 для ddl_template clickhouse
    logger.info(f'Generating ddl {schema_ch}.{distributed_table}')
    context = {
        "schema_ch": schema_ch,
        "distributed_table": distributed_table,
        "replicated_table": replicated_table,
        "sharding_key_ch": sharding_key_ch,
        "schema_hive": schema_hive,
        "hive_table": hive_table,
        "cluster": cluster
    }

    # Получаем имена колонок и их типы
    df_describe = sc.sql(f'''describe table {schema_hive}.{hive_table} ''').toPandas()
    # logger.info(f'Result describe hive table {schema_hive}.{hive_table}: {df_describe}')

    # Формируем columns для ddl таблицы в кликхаусе
    # Получим колонки отдельно от метаинформации о партициях
    try:
        target_index = df_describe[df_describe['col_name']=='# Partition Information'].index[0]
        df_describe_columns = df_describe.iloc[:target_index]
    except:
        print("Partitions doesn't exists")
        df_describe_columns = df_describe.iloc[:] # df колонок без меты

    # Формируем список колонок и их типы для ddl таблицы в clickhouse
    lst_columns_types = []
    for _, row in df_describe_columns.iterrows():
        if row['col_name'] == sharding_key_ch:
            # Без Nullable
            lst_columns_types.append(f"`{row['col_name']}` {hive_to_clickhouse_type(hive_type=row['data_type'])} COMMENT '{row['comment']}'")
        elif row['col_name'] != sharding_key_ch:
            # Оставляем Nullable
            lst_columns_types.append(f"`{row['col_name']}` Nullable({hive_to_clickhouse_type(hive_type=row['data_type'])}) COMMENT '{row['comment']}'")
    columns_types = ',\n'.join(lst_columns_types)
    context["columns_types"] = columns_types

    logger.info(f'Result columns hive table {schema_hive}.{hive_table}: {context["columns_types"]}')

    # Загружаем шаблон
    env = Environment(loader=FileSystemLoader('./sql/'), undefined=StrictUndefined)

    # Рендерим и сохраняем
    logger.info(f'Trying render ddl_template sql/ddl_template.sql.template')
    template = env.get_template('ddl_template.sql.template')
    output = template.render(**context)

    with open(f'sql/{context["distributed_table"]}.sql', 'w', encoding='utf-8') as f:
        f.write(output)
    logger.info(f'Downloaded ddl sql/{context["distributed_table"]}.sql')
    logger.info(output)
    logger.info(f'You could copy ddl from sql/{context["distributed_table"]}.sql or from console :D')

    return


def hive_to_clickhouse_type(hive_type: str) -> str:
    hive_type = hive_type.upper()
    type_mapping = {
        "BOOLEAN": "UInt8",
        "TINYINT": "Int8",
        "SMALLINT": "Int16",
        "INT": "Int32",
        "INTEGER": "Int32",
        "BIGINT": "Int64",
        "FLOAT": "Float32",
        "DOUBLE": "Float64",
        "DECIMAL": "Decimal",
        "STRING": "String",
        "VARCHAR": "String",
        "CHAR": "FixedString",
        "BINARY": "String",
        "DATE": "Date",
        "TIMESTAMP": "DateTime64(6)",
        "ARRAY": "Array",
        "MAP": "Tuple",
        "STRUCT": "Tuple",
    }

    # Обработка составных типов (например, DECIMAL(10,2))
    if "(" in hive_type:
        base_type = hive_type.split("(")[0].upper()
        if base_type in type_mapping:
            return hive_type.replace(base_type, type_mapping[base_type])

    return type_mapping.get(hive_type.upper(), "String")  # fallback to String

main()