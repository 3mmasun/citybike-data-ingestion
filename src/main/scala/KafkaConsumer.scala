
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
        .add("free_bikes",ShortType)
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

//    import spark.implicits._
    val results = topicDF
      .select(from_json(col("value").cast("string"), networkSchema).as("city"))

    results.show()

    results
      .write.mode(SaveMode.Overwrite)
      .parquet("/Users/sgemma.sun/hadoop-3.1.3/" + "user/emma/citibike")
  }

}
