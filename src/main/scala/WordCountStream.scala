import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object WordCountStream {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Word Count in Stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val streamDF: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val wordsDF: DataFrame = streamDF
      .withColumn("wordArray", split($"value", " "))
      .withColumn("word", explode($"wordArray"))

    val countDF = wordsDF
      .groupBy($"word")
      .agg(count("value").alias("Count"))

    val checkpointDir = "temp"

    val streamQuery = countDF.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", checkpointDir)
      .start()

    streamQuery.awaitTermination()
  }

}
