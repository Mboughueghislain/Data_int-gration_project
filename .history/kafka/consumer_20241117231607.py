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
    .option("path", "hdfs://localhost:9000/hospital_data/received") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/hospital_data") \
    .start()

query.awaitTermination()

# Extract the "data" field (which contains the actual CSV-like data)
data = df.select("data").collect()[0][0]

# Split the data into rows
rows = [row.strip() for row in data.split("\\n") if row]

# Convert each row into a list (to simulate CSV columns)
rows_split = [row.split(",") for row in rows]

# Define the schema for the DataFrame
columns = ["Department", "Organization", "Hospital", "Date", "Value"]

# Create a DataFrame from the split data
hospital_df = spark.createDataFrame(rows_split, columns)

# Show the first few rows of the DataFrame (for verification)
hospital_df.show()

# Save the DataFrame to HDFS (final_output folder)
output_path = "hdfs://localhost:9000/hospital_data/final_output"
hospital_df.write.csv(output_path, header=True, mode="overwrite")

