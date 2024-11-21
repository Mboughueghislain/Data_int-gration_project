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

# Extraire le contenu des messages Kafka (en supposant qu'ils sont envoyés en JSON ou CSV)
streaming_data = kafka_df.selectExpr("CAST(value AS STRING)").alias("value")

# Parse le CSV provenant du flux
schema = "Date STRING, Setting STRING, Category STRING, System STRING, `Facility Name` STRING, Count INT"
parsed_stream = streaming_data.selectExpr(f"from_csv(value, '{schema}') as data").select("data.*")

# Convertir la colonne 'Date' en type date
parsed_stream = parsed_stream.withColumn("Date", to_date(col("Date"), "MMM-yy"))

# ---- Étape 3 : Traitement des données ----
# Jointure entre le flux Kafka et la table 'hospital_data'
join1 = parsed_stream.join(hospital_data, ["Date", "Category", "Setting"], "left_outer")

# Jointure entre le résultat précédent et 'health_category'
final_output = join1.join(health_category, ["Date", "Category", "Setting"], "left_outer")

# Ajouter des colonnes supplémentaires pour l'analyse si nécessaire (exemple : calculer les totaux)
final_output = final_output.withColumn("Total_Count", col("Count") + lit(0))  # Exemple de transformation

# Définir les chemins pour le HDFS
output_path = "hdfs://localhost:9000/hospital_data/final_output"
checkpoint_path = "hdfs://localhost:9000/hospital_data/checkpoints"

# Écrire le résultat final dans le HDFS (flux continu)
final_output = fina\
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

# Attendre la fin du streaming
final_output.awaitTermination()
