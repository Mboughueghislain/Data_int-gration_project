from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import split, col, lit

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

# Lire les données depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Décoder les messages Kafka (clé et valeur) en tant que chaînes de caractères
decoded_df = kafka_df.selectExpr("CAST(value AS STRING) AS csv_value")

# Diviser la chaîne CSV en colonnes (en supposant que les données sont séparées par des virgules)
csv_split_df = decoded_df.withColumn("csv_columns", 
                                    split(col("csv_value"), ",").cast("array<string>"))

# Extraire les valeurs dans des colonnes structurées
structured_df = csv_split_df.select(
    col("csv_columns").getItem(0).alias("department"),
    col("csv_columns").getItem(1).alias("hospital"),
    col("csv_columns").getItem(2).alias("hospital_name"),
    col("csv_columns").getItem(3).alias("timestamp"),
    col("csv_columns").getItem(4).alias("trend_value")
)

# Affichage des premières lignes pour vérifier que les données sont traitées
structured_df.show(truncate=False)

# Exemple de traitement : ajouter une colonne calculée
processed_df = structured_df \
    .withColumn("processed_trend", col("trend_value").substr(0, 10)) \
    .withColumn("processing_timestamp", lit("2024-11-18"))

# Définir les chemins pour le HDFS
output_path = "hdfs://localhost:9000/hospital_data/final_output"
checkpoint_path = "hdfs://localhost:9000/hospital_data/checkpoints"

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
