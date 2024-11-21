from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, lit

# Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkConsumerWithProcessing") \
    .getOrCreate()

# Définir les paramètres Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "hospital_trends"

# Vérifier que le topic existe avant de consommer (optionnel mais conseillé)
try:
    from kafka import KafkaAdminClient
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    topics = admin_client.list_topics()
    if kafka_topic not in topics:
        raise ValueError(f"Le topic '{kafka_topic}' n'existe pas dans Kafka.")
    admin_client.close()
except Exception as e:
    print(f"Erreur lors de la vérification du topic : {e}")
    exit(1)

# Définir un schéma pour les messages JSON consommés
schema = StructType() \
    .add("hospital", StringType()) \
    .add("department", StringType()) \
    .add("trends", StringType()) \
    .add("timestamp", StringType())

# Lire les données depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Décoder les messages Kafka (clé et valeur) de type JSON
decoded_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

# Appliquer le schéma pour convertir les messages en DataFrame structuré
structured_df = decoded_df \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Exemple de traitement : ajouter une colonne calculée
processed_df = structured_df \
    .withColumn("processed_trend", col("trends").substr(0, 10)) \
    .withColumn("processing_timestamp", lit("2024-11-18"))

# Définir les chemins pour le HDFS
output_path = "hdfs://localhost:9092/hospital_data/final_output"
checkpoint_path = "hdfs://localhost:9092/hospital_data/checkpoints"

# Écrire le résultat final dans le HDFS
final_output = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

# Attendre la fin du streaming
final_output.awaitTermination()