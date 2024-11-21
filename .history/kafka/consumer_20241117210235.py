from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json
from pyspark.streaming import StreamingContext

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

ssc = StreamingContext(spark.sparkContext, 10)  # Batch interval: 10 seconds

kafka_stream = KafkaUtils.createDirectStream(
    ssc,
    topics=['hospital_trends'],
    kafkaParams={"metadata.broker.list": "localhost:9092"}
)

def process_rdd(rdd):
    if not rdd.isEmpty():
        data = rdd.map(lambda x: x[1])
        df = spark.read.json(data)
        df.write.mode("append").csv("hdfs://localhost:9000/hospital_data")

kafka_stream.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()