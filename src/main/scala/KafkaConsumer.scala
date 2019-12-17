
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val networkSchema = new StructType()
      .add("company", ArrayType(StringType))
      .add("gbfs_href",StringType)
      .add("href",StringType)
      .add("id",StringType)
      .add("location",
        new StructType()
          .add("city",StringType)
          .add("country", StringType)
          .add("latitude", FloatType)
          .add("longitude", FloatType)
      )
      .add("name",StringType)
      .add("stations", ArrayType(
        new StructType()
          .add("empty_slots", ShortType)
          .add("free_bikes",ShortType)
          .add("id",StringType)
          .add("latitude", FloatType)
          .add("longitude", FloatType)
          .add("name", StringType)
          .add("timestamp", StringType)
          .add("extra",
            new StructType()
              .add("last_updated", IntegerType)
              .add("renting", ShortType)
              .add("returning", ShortType)
              .add("uid", StringType)
              .add("address", StringType)
          )
      )
      )

    val spark = SparkSession
      .builder()
      .appName("CitibikeIngest")
      .master("local[*]")
      .getOrCreate()

    val topicDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val results = topicDF.select(from_json(col("value").cast("string"), networkSchema).as("city"))
      .select("city.*")

    val query = results
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "/Users/sgemma.sun/hadoop-3.1.3/hadoop_store/tmp/checkpoint")
      .option("path", "./parquet_data")
//      .option("path", "hdfs://localhost:9000/parquet_data")
      .start()

    query.awaitTermination()

  }

}
