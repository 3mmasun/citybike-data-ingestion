
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.{StringDeserializer}


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("CityBikeIngestion").setMaster("local[*]")
//    val streamingContext = new StreamingContext(conf, Seconds(1))
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "datalake",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//
//    val topics = Array("test")
//    val offsetRanges = Array(
//      // topic, partition, inclusive starting offset, exclusive ending offset
//      OffsetRange("test", 0, 100, 102)
//    )
//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//
//    val result = stream.map(record => (record.key, record.value))
//    print(result)
    val spark = SparkSession.builder().appName("testapp").master("local[*]").getOrCreate()
    val topicDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "{\"test\":{\"0\":895}}")
      .option("kafka.key.deserializer", classOf[StringDeserializer].getName)
      .load()

    val results = topicDF.collect()
    printf("AAAAAAAAAAAAAAAAAAA")
    results.map(print)
    printf("BBBBBBBBBBBBBBBBBBB")
  }

}
