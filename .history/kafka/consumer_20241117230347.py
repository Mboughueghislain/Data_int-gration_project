from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_json, col

# Créer une SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

# Lire les données de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital_trends") \
    .load()

# Convertir la valeur des messages Kafka (qui est en bytes) en chaîne de caractères
df_parsed = df.selectExpr("CAST(value AS STRING) as value")

# Sauvegarder les données dans HDFS
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/hospital_data/") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/hospital_data") \
    .start()

query.awaitTermination()
