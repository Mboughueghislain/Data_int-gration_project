from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HospitalDataProcessing").getOrCreate()

# Load streaming data
hospital_df = spark.read.csv("hdfs://localhost:9000/hospital_data/hospital-utilization-trends.csv", header=True, inferSchema=True)

# Load static data
diagnosis_df = spark.read.csv("hdfs://localhost:9000/hospital_data/in-hospital-mortality-trends-by-diagnosis-type.csv", header=True, inferSchema=True)
health_category_df = spark.read.csv("hdfs://localhost:9000/hospital_data/in-hospital-mortality-trends-by-health-category.csv", header=True, inferSchema=True)

# Join datasets
final_df = hospital_df.join(diagnosis_df, "common_column", "inner").join(health_category_df, "common_column", "inner")

final_df.write.mode("overwrite").parquet("hdfs://localhost:9000/hospital_data/final_output")