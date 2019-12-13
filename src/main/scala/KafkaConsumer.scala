
import org.apache.spark.sql.SparkSession

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
      .selectExpr("CAST(value AS STRING) as raw_payload")

    topicDF.printSchema()

    val results = topicDF.collect()
    results.map(print)
  }

}
