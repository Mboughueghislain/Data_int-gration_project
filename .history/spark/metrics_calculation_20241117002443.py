from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MetricsCalculation").getOrCreate()

final_df = spark.read.parquet("hdfs://localhost:9000/hospital_data/final_output")

# Calculate metrics
metrics_df = final_df.groupBy("category").agg(
    {"mortality_rate": "avg", "hospitalizations": "sum"}
)
metrics_df.show()

# Store metrics
metrics_df.write.mode("overwrite").csv("hdfs://localhost:9000/hospital_data/metrics")