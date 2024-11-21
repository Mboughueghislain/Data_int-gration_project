import org.apache.spark.sql.SparkSession

object ReadParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Read Parquet File")
      .master("local[*]")  // Si vous travaillez en mode local
      .getOrCreate()

    // Lecture du fichier Parquet depuis HDFS
    val df = spark.read.parquet("hdfs://localhost:9092/hospital_data/final_output/part-00000-08a9ad5a-b91e-4435-999a-d1566a339909-c000.snappy.parquet")
    df.show()
  }
}
