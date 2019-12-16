package spark.scala

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}

object DatasetOperations {
  val spark = SparkSession.builder().appName("SparkDataset").config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.join.preferSortMergeJoin", "false").config("spark.sql.autoBroadcastJoinThreshold", "1").config("spark.eventLog.enabled", true).master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  def createDataset(): DataFrame = {
    val dataset = spark.read.format("avro").load("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/avro/part-00000-bfc36d17-65fb-4ca6-9ae9-8ece066adc62-c000.avro")
    dataset
  }

  def narrow() = {
    import spark.implicits._
    // cache the dataset
    val dataset = createDataset().cache()

    // Filter by column index number
    dataset.filter(v => v.get(0) != null).filter(v => v.get(0).toString.equals("UA")).show(false)

    // Filter by column name
    dataset.filter($"IATA_CODE" === "UA").show(false)

    // We can also define condition expression
    dataset.filter("IATA_CODE is not null and IATA_CODE == 'UA'").show(false)

    // select or selectExpr
    dataset.selectExpr("IATA_CODE").show(false)
    dataset.select($"AIRLINE").show(1, false)

    // as (alias name for a column)
    val columnAliasDataset = dataset.select($"AIRLINE".as("aliasName"))
    columnAliasDataset.show(1, false)

    // Add new column with timestampø
    val newdataset = dataset.withColumn("newColumn", org.apache.spark.sql.functions.current_timestamp())
    newdataset.show(2, false)

    // Rename existing column name
    newdataset.withColumnRenamed("newColumn", "timestamp").show(1, false)

    // where clause
    newdataset.where("newColumn > '2019-12-08 12:18:31.059'").show(false)
  }

  def checkpoint() = {
    import spark.implicits._
    spark.sparkContext.setCheckpointDir("/Users/ukannika/work/choice/git/personal/spark-scala/checkpointDir/")
    val dataset = createDataset()
    dataset.select($"AIRLINE").checkpoint(true)
  }

  def cache(): Unit = {
    import spark.implicits._
    val dataset = createDataset().filter($"IATA_CODE" === "US").cache()
    dataset.select($"AIRLINE").show()
  }

  def printSchema() = {
    val dataset = createDataset()
    dataset.schema.printTreeString()
  }

  def coalesce() = {
    val dataset = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      // Infer schema -> it needs to run once to find the schema. So we may see multiple stages.
      .option("enforceSchema", false).option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/flights/flights.csv")

    // Coalesce from 8 to 4 partitions
    dataset.coalesce(4).foreachPartition((iterator: Iterator[org.apache.spark.sql.Row]) => println("====> " + iterator.length))
  }

  def repartition() = {
    val dataset = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      // Infer schema -> it needs to run once to find the schema. So we may see multiple stages.
      .option("enforceSchema", false).option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/flights/flights.csv")

    // Coalesce from 8 to 4 partitions
    dataset.repartition(10).foreachPartition((iterator: Iterator[org.apache.spark.sql.Row]) => println("====> " + iterator.length))
  }

  def join() = {
    val tweets = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/TweetDataCleaned.csv")

    val users = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/UserDataCleaned.csv")

    tweets.join(users, tweets("User_ID") === users("USER_ID"), "leftsemi")
      .select(tweets("USER_ID")).show()


    tweets.join(users, tweets("User_ID") === users("USER_ID"), "leftsemi")
      .select(tweets("USER_ID")).show()

    tweets.join(users, tweets("User_ID") === users("USER_ID"), "leftsemi")
      .select(tweets("USER_ID")).show()
  }

  def joinWithBucketing() = {
    import spark.implicits._
    val tweets = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "DROPMALFORMED")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/TweetDataCleaned.csv")

    val users = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", true)
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/UserDataCleaned.csv")

    tweets.select($"User_ID").write.bucketBy(1, "USER_ID").saveAsTable("tweets")
    users.selectExpr("USER_ID, Name").write.bucketBy(1, "USER_ID").saveAsTable("users")
  }


  def map() = {
    import spark.implicits._
    val dataset = Seq(1, 2, 3).toDS()
    dataset.withColumn("value", functions.col("value").cast("Long")).schema.printTreeString()
  }

  def main(args: Array[String]): Unit = {
    map()
  }

  // Airline case class
  case class Airline(iata_code: String, airline: String)

}
