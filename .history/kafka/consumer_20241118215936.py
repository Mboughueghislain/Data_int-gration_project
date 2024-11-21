from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Création de la session Spark
spark = SparkSession.builder \
    .appName("HospitalDataIntegration") \
    .getOrCreate()

# Lecture des fichiers statiques depuis HDFS
diagnosis_df = spark.read.csv("hdfs://localhost:9000/hospital_data/in-hospital-mortality-trends-by-diagnosis-type.csv", header=True, inferSchema=True)
health_category_df = spark.read.csv("hdfs://localhost:9000/hospital_data/in-hospital-mortality-trends-by-health-category.csv", header=True, inferSchema=True)

# Lecture en streaming depuis Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital-data") \
    .load()

df_stream = df_stream.selectExpr("CAST(value AS STRING)")

# Transformation des données
def process_batch(batch_df, batch_id):
    # Transformation des données reçues
    incoming_data = spark.read.csv(batch_df.rdd.map(lambda x: x.value), header=True, inferSchema=True)

    # Jointure avec les fichiers statiques
    enriched_df = incoming_data \
        .join(diagnosis_df, ["diagnosis_type"], "left") \
        .join(health_category_df, ["health_category"], "left") \
        .withColumn("average_mortality", (col("mortality_rate_x") + col("mortality_rate_y")) / 2)

    # Sauvegarde des données enrichies dans HDFS
    enriched_df.write.mode("append").parquet("hdfs://localhost:9000/hospital_data/final_output/")

# Configuration du streaming
df_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://localhost:9000/hospital_data/received/") \
    .start() \
    .awaitTermination()