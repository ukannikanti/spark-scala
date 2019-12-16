package spark.scala

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SParkStructuredStreaming {
  val spark = SparkSession.builder().appName("SparkStructuredStreaming")
    .config("spark.eventLog.enabled", true).master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/Users/ukannika/work/choice/git/personal/spark-scala/checkpointLocation")
    .getOrCreate()

  def socketSourceToConsoleSink() = {
    import spark.implicits._
    val lines = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()

    // We have now set up the query on the streaming data.
    // Now will need to write the results of this query to output sink.
    // Results can be written to output sink can be 3 types. 1. Append 2. Update 3. Complete
    val query = words.writeStream.outputMode(OutputMode.Update()).format("console").trigger(Trigger.ProcessingTime(5000)).start()

    // Will need to wait for query to be executed incrementally..
    // Inorder to stop this, will need to call query.stop() or any exception should happen
    query.awaitTermination()
  }

  def kafkaSourceToKafka() = {
    import spark.implicits._

    val count = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .selectExpr("value").as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .selectExpr("CAST(value AS STRING) as key ", "CAST(count AS STRING) as value")

    //Write to Kafka
    val query = count
      .writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "results")
      .trigger(Trigger.ProcessingTime(5000))
      .start()

    query.awaitTermination()
  }

  def windowAggregations() = {
    import spark.implicits._

    val words = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .as[(String, String, Timestamp)]
      .selectExpr("value", "timestamp").as[(String, Timestamp)]

    val counts = words
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp", "1 minutes", "30 seconds"),
        $"value").count()

    val query = counts.writeStream.outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime(5000))
      .start()

    query.awaitTermination()
  }

  def mapGroupsWithState() = {

  }

  def main(args: Array[String]): Unit = {
    windowAggregations()
  }
}
