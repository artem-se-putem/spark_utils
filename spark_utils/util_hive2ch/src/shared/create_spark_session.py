from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """Создает Spark сессию с базовыми настройками"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("yarn") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark