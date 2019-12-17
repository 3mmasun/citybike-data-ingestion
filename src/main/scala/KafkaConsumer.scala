
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testapp").master("local[*]").getOrCreate()
    val topicDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "{\"test\":{\"0\":895}}")
      .load()

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
//        .add("extra",
//          new StructType()
//            .add("last_updated", IntegerType)
//            .add("renting", ShortType)
//            .add("returning", ShortType)
//            .add("uid", StringType)
//            .add("address", StringType)
//          )
        )
      )

    val results = topicDF.select(from_json(col("value").cast("string"), networkSchema)
      .as("city"))
        .select("city.*")

    results.show()

//    import spark.implicits._
//    val someDF = Seq(
//      (8, "bat"),
//      (64, "mouse"),
//      (-27, "horse")
//    ).toDF("number", "word");
//
//
    results
      .write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://localhost:9000/parquet_data")


    val df_parquet = spark
      .read
      .parquet("hdfs://localhost:9000/parquet_data")

    df_parquet.show()
  }

}
