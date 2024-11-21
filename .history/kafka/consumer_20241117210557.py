from pyspark.sql import SparkSession

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Lire les données depuis Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital_trends") \
    .load()

# Convertir les messages Kafka (clé et valeur) en chaînes de caractères
data_stream = kafka_stream.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value"
)

# Afficher les données dans la console pour vérification
query = data_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()