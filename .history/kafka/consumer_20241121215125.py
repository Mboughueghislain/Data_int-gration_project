from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import split, col, lit,to_date

# Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkConsumerWithProcessing") \
    .getOrCreate()

# ---- Étape 1 : Lecture des tables statiques depuis HDFS ----
hospital_data_path = "hdfs:///path/to/in-hospital-mortality-trends-by-diagnosis-type.csv"
health_category_path = "hdfs:///path/to/in-hospital-mortality-trends-by-health-category.csv"

# Charger les tables statiques
hospital_data = spark.read.option("header", "true").csv(hospital_data_path)
health_category = spark.read.option("header", "true").csv(health_category_path)

# Convertir les colonnes 'Date' en type date pour uniformiser les jointures
hospital_data = hospital_data.withColumn("Date", to_date(col("Date"), "MMM-yy"))
health_category = health_category.withColumn("Date", to_date(col("Date"), "MM/yyyy"))

# Définir les paramètres Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "hospital_trends"

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

# Exemple de traitement : ajouter une colonne calculée
processed_df = structured_df \
    .withColumn("processed_trend", col("trend_value").substr(0, 10)) \
    .withColumn("processing_timestamp", lit("2024-11-18"))

# Définir les chemins pour le HDFS
output_path = "hdfs://localhost:9000/hospital_data/final_output"
checkpoint_path = "hdfs://localhost:9000/hospital_data/checkpoints"

# Écrire le résultat final dans le HDFS (flux continu)
final_output = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

# Attendre la fin du streaming
final_output.awaitTermination()
