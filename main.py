import os
import tempfile
from pyspark.sql import SparkSession

# Настройка Python
os.environ['PYSPARK_PYTHON'] = 'C:\\Python310\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Python310\\python.exe'

# Настройка Hadoop (опционально, для устранения предупреждения)
hadoop_temp = tempfile.mkdtemp()
os.makedirs(os.path.join(hadoop_temp, 'bin'), exist_ok=True)
os.environ['HADOOP_HOME'] = hadoop_temp

# Создание SparkSession
spark = SparkSession.builder \
    .appName("LocalSparkSession") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("\n" + "="*50)
print("✅ SPARK УСПЕШНО ЗАПУЩЕН")
print("="*50)
print(f"Версия Spark: {spark.version}")
print(f"Режим работы: {spark.sparkContext.master}")

# Работа с данными
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

print("\n📊 Данные DataFrame:")
df.show()

print(f"\n📈 Количество записей: {df.count()}")

print("\n🔍 Люди старше 30:")
df.filter(df.Age > 30).show()

print("\n📋 Схема данных:")
df.printSchema()

# Остановка Spark
spark.stop()
print("\n👋 SparkSession остановлена")

tempfile.