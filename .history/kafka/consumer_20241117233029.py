from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Créer une SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

# Définir le schéma pour les données JSON que nous attendons depuis Kafka
schema = StructType([
    StructField("Department", StringType(), True),
    StructField("Organization", StringType(), True),
    StructField("Hospital", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Value", StringType(), True)
])

# Lire les données de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital_trends") \
    .load()

# Convertir la valeur des messages Kafka (en bytes) en chaîne de caractères
df_parsed = df.selectExpr("CAST(value AS STRING) as value")
df_parsed
# Convertir la chaîne JSON en DataFrame structuré en utilisant le schéma défini
df_json = df_parsed.select(from_json(col("value"), schema).alias("data"))

# Exploser les données dans un DataFrame avec les colonnes séparées
df_final = df_json.select("data.*")
df_final.show()
# Sauvegarder les données brutes dans "received"
query_received = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/hospital_data/received") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/hospital_data_received") \
    .start()

# Sauvegarder les données transformées dans "final_output"
query_final_output = df_final.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/hospital_data/final_output") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/hospital_data_final_output") \
    .start()

# Attendre que les deux flux de données terminent
query_received.awaitTermination()
query_final_output.awaitTermination()

# Arrêter la SparkSession après la fin du traitement
spark.stop()

