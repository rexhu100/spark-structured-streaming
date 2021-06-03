import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCountStream {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Word Count in Stream").getOrCreate()

    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val streamingQuery: StreamingQuery = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    streamingQuery.awaitTermination()
  }

}
