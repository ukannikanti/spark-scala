package spark.scala

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession, functions}

object SparkDataset {
  val spark = SparkSession.builder().appName("SparkDataset").master("local[*]")
    .config("hive.exec.dynamic.partition", "true").
    config("hive.exec.dynamic.partition.mode", "nonstrict").
    config("hive.execution.engine", "spark"). // Use Tez for Hive Bucketing semantics
    config("hive.exec.max.dynamic.partitions", "400").
    config("hive.exec.max.dynamic.partitions.pernode", "400").
    config("hive.enforce.bucketing", "true").
    config("optimize.sort.dynamic.partitionining", "true").
    config("hive.vectorized.execution.enabled", "true").
    config("hive.enforce.sorting", "true").
    enableHiveSupport().enableHiveSupport().getOrCreate()

  def createDataset() = {
    import spark.implicits._
    // 1. read from external sources like above -> csv, json, parquet, JDBC, Hive, HBase, etc.
    // 2. In spark sql execution engine, as data converted to spark internal format, will need to use Encoders
    // to serialize the objects for processing or transmitting over the network.
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Primitive type dataset
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()

    spark.createDataset(Seq(new Person("name", 2)))(Encoders.product[Person]).show()
    spark.createDataset(List(new Person("name", 2)))(Encoders.product[Person]).show()
  }

  def rddToDataSet() = {
    import spark.implicits._
    // There are 2 Ways to convert RDD to Dataset.
    // The first method uses reflection to infer the schema of an RDD/List/Seq that contains specific types of objects.
    val rdd: org.apache.spark.rdd.RDD[String] = spark.sparkContext.parallelize(Seq("name, 1"))
    rdd.map(_.split(","))
      .map(v => new Person(v(0), v(1).trim.toInt))
      .toDS()
      .show(false)

    // The second method for creating Datasets is through a programmatic interface that allows you
    // to construct a schema and then apply it to an existing RDD.
    // While this method is more verbose, it allows you to construct Datasets
    // when the columns and their types are not known until runtime.
    val rdd1: org.apache.spark.rdd.RDD[String] = spark.sparkContext.parallelize(Seq("second test, 1"))
    // Here we assume that we don't know the schema till run time. At runtime our schema looks like below.
    // The schema is encoded in a string
    val schemaString = "name,age"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = rdd1
      .map(_.split(","))
      .map(attributes => org.apache.spark.sql.Row(attributes(0), attributes(1).trim))

    val dataset = spark.createDataset(rowRDD)(org.apache.spark.sql.catalyst.encoders.RowEncoder(schema))

    dataset.show()
  }

  def readCSVAndSaveAsHiveTable() = {
    // Read from CSV
    val dataset = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("enforceSchema", false).option("inferSchema", true).option("samplingRatio", 0.7)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/flights/flights.csv")

    dataset
      .write
      .bucketBy(2, "CITY")
      .partitionBy("IATA_CODE")
      .saveAsTable("flights_partitioned_bucketed")
  }

  def readCSVAndSaveAsORC() = {
    // Read from CSV
    val dataset = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("enforceSchema", false).option("inferSchema", true).option("samplingRatio", 0.7)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/flights/flights.csv")

    dataset
      .write
      .partitionBy("AIRLINE")
      .mode(SaveMode.Overwrite)
      .orc("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/orc/")
  }

  def userDefinedFunctions() = {
    val squared = functions.udf((s: Long) => s * s)
    val dataset = spark.range(1, 20)
    dataset.select(squared(functions.col("id")) as "id_squared").show(false)

    //Inorder to use in spark SQL will need to register with Spark
    dataset.createOrReplaceTempView("table1")
    spark.udf.register("squared", squared)
    spark.sql("select squared(id) as id from table1").show(false)
  }

  def events() = {
    val eventsDataset = spark.read.option("sep", ",").option("quote", "\"").option("inferSchema", true).option("header", true).csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/event_clean_cat.csv")
    val tweetDataset = spark.read.option("sep", ",").option("quote", "\"").option("inferSchema", true).option("header", true).csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/TweetDataCleaned.csv")
    val userDataset = spark.read.option("sep", ",").option("quote", "\"").option("inferSchema", true).option("header", true).csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/events/UserDataCleaned.csv")
    eventsDataset.createOrReplaceTempView("events")
    tweetDataset.createOrReplaceTempView("tweets")
    userDataset.createOrReplaceTempView("users")
    println(spark.sql("select e.event_id, t.User_ID, u.User_Location from events e INNER JOIN tweets t ON e.lat >= t.lat - 5 AND e.lat <= t.lat + 5 INNER JOIN users u ON u.ID == t.User_ID").count())
  }

  def csvToAvro() = {
    val dataset = spark.read.option("sep", ",").option("quote", "\"").option("header", true)
      .option("enforceSchema", false).option("inferSchema", true).option("samplingRatio", 0.7)
      .option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .csv("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/csv/flights/airlines.csv")

    dataset.write.mode(SaveMode.Append).format("avro").save("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/avro")
  }

  def avro() = {
    val avroSchema = new String(Files.readAllBytes(Paths.get("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/avro/avro.avsc")))
    val dataset = spark.read.option("ignoreExtension", false).option("avroSchema", avroSchema).format("avro").load("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/avro/")
    dataset.show(false)

    // To view avro schema
    println(SchemaConverters.toAvroType(dataset.schema))
  }



  def main(args: Array[String]): Unit = {
    avro()
  }

  case class Person(name: String, age: Long)

}
