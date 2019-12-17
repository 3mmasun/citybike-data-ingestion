import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HDFSReader {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("CitibikeHDFSReader")
      .master("local[*]")
      .getOrCreate()

    val df_parquet = spark
      .read
      .parquet("./parquet_data")

    df_parquet.show()
  }
}
