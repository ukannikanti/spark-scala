package spark.scala

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object SparkRDD {
  val spark = SparkSession.builder()
    .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=INFO,console")
    .master("local[*]")
    .appName("SparkRDD")
    .getOrCreate()

  def createWeatherRDD(): RDD[GenericRowWithSchema] = {
    val schema = StructType(
      List(
        StructField("date", DataTypes.TimestampType, true),
        StructField("temperaturemin", DataTypes.StringType, true),
        StructField("temperaturemax", DataTypes.StringType, true),
        StructField("precipitation", DataTypes.StringType, true),
        StructField("snowfall", DataTypes.StringType, true),
        StructField("snowdepth", DataTypes.StringType, true),
        StructField("avgwindspeed", DataTypes.StringType, true),
        StructField("fastest2minwinddir", DataTypes.StringType, true),
        StructField("fastest2minwindspeed", DataTypes.StringType, true),
        StructField("fog", DataTypes.StringType, true),
        StructField("fogheavy", DataTypes.StringType, true),
        StructField("mist", DataTypes.StringType, true),
        StructField("rain", DataTypes.StringType, true),
        StructField("fogground", DataTypes.StringType, true),
        StructField("ice", DataTypes.StringType, true),
        StructField("glaze", DataTypes.StringType, true),
        StructField("snow", DataTypes.StringType, true),
        StructField("freezingrain", DataTypes.StringType, true),
        StructField("smokehaze", DataTypes.StringType, true),
        StructField("thunder", DataTypes.StringType, true),
        StructField("highwind", DataTypes.StringType, true),
        StructField("hail", DataTypes.StringType, true),
        StructField("blowingsnow", DataTypes.StringType, true),
        StructField("dust", DataTypes.StringType, true),
        StructField("freezingfog", DataTypes.StringType, true)
      )
    )

    val temperatureRDD = spark.sparkContext.textFile("/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/flights/*.csv", 10)
    temperatureRDD.filter(value => !value.startsWith("date")).mapPartitionsWithIndex((index, partition) => {
      val list = partition.toList
      list.map(v => {
        val array = v.split(";").map(_.asInstanceOf[Any])
        new GenericRowWithSchema(array, schema)
      }).iterator
    })
  }

  def narrowTransformations() = {
    val data = Array(1, 2, 3, 4, 5)
    val rdd = spark.sparkContext.parallelize(data, 5)

    val data1 = Array(5, 6, 7, 8, 9, 10)
    val rdd1 = spark.sparkContext.parallelize(data1, 5)

    // map
    rdd.map(v => v + 1).collect().foreach(v => println(v))

    // filter
    rdd.filter(v => v == 1).collect().foreach(v => println(v))

    // flatMap
    val flatmapRDD = spark.sparkContext.parallelize(List("spark rdd example", "sample example"), 2)
    flatmapRDD.flatMap(value => value.split(" ")).collect().foreach(v => println(v))

    // mapPartitions
    rdd.mapPartitions(partition => {
      val list = partition.toList
      list.map(v => v + 10).iterator
    }).collect().foreach(v => println(v))

    // mapPartitionsWithIndex
    rdd.mapPartitionsWithIndex((index, partition) => {
      println("Index  ==> " + index)
      val list = partition.toList
      list.map(v => v + 100).iterator
    }).collect().foreach(v => println(v))

    //sample
    rdd.sample(true, 0.2).collect().foreach(v => println(v))
    rdd.sample(false, 0.5).collect().foreach(v => println(v))

    //distinct
    rdd.union(rdd1).distinct().collect().foreach(v => println(v))

    //Union
    rdd.union(rdd1).collect().foreach(v => println(v))

    // If we run this => it will only create stages for every collect action. (9 stages total)
  }

  // These operations may cause shuffle.
  def wideTransformations() = {
    // Join
    val emp = spark.sparkContext.parallelize(Seq(("jordan", 10), ("jordan", 40), ("ricky", 20), ("matt", 30), ("mince", 35), ("rhonda", 30)))
    val dept = spark.sparkContext.parallelize(Seq(("hadoop", 10), ("spark", 20), ("hive", 30), ("sqoop", 40)))
    val transformedEmp = emp.map(t => (t._2, t._1))
    val transformedDept = dept.map(t => (t._2, t._1))
    transformedEmp.join(transformedDept).collect foreach println

    // intersection
    transformedEmp.intersection(transformedDept).collect foreach println

    //reduceByKey
    emp.reduceByKey((a, b) => a + b).collect().foreach(v => println(v))

    // groupByKey
    emp.groupByKey().collect foreach println
    // groupByKey with aggregation
    emp.groupByKey().mapValues(v => v.sum).collect foreach println
  }

  def wideTransformationsOnWeatherRDD() = {
    val rdd = createWeatherRDD()
    // calculate number of records per year
    // groupBy year
    rdd.groupBy(v => LocalDate.parse(v.getString(0)).getYear).mapValues(_.size).collect foreach println
    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, v)).groupByKey().collect foreach println

    //reduceByKey
    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, 1)).reduceByKey((a, b) => a + b).collect().foreach(v => println(v))

    // Get minimum temperature by year using combineByKey.
    def createCombiner = (x: GenericRowWithSchema) => x.getString(1).toDouble

    // A function to merge
    def mergeValue = (accumulator: Double, x: GenericRowWithSchema) => accumulator min x.getString(1).toDouble

    // Combiner to merge
    def mergeCombiner = (accumulator1: Double, accumulator2: Double) =>
      accumulator1 min accumulator2
    // use combineByKey for finding min
    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, v))
      .combineByKey(createCombiner, mergeValue, mergeCombiner)
      .collect foreach println

    //use aggregateByKey to find min temperature
    def aggMergeValue = (accumulator: Double, x: GenericRowWithSchema) => accumulator min x.getString(1).toDouble

    def aggMergeCombiner = (accumulator1: Double, accumulator2: Double) =>
      accumulator1 min accumulator2

    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, v))
      .aggregateByKey((1000.0))(aggMergeValue, aggMergeCombiner)
      .collect foreach println

    // Use aggregateByKey to find average min temperature
    def avgMergeValue = (accumulator: (Double, Int), x: GenericRowWithSchema) => (accumulator._1 + x.getString(1).toDouble, accumulator._2 + 1)

    def avgMergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, v))
      .aggregateByKey((1000.0, 0))(avgMergeValue, avgMergeCombiner)
      .mapValues(v => 1.0 * v._1 / v._2)
      .collect foreach println

    //sortByKey
    rdd.map(v => (v.getString(0), v)).sortByKey(false).collect foreach println
  }

  def shuffleTest() = {
    val path = "/Users/ukannika/work/choice/git/personal/spark-scala/src/main/resources/output/";
    val rdd = createWeatherRDD()
    //rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, 1)).reduceByKey((a, b) => a + b).saveAsTextFile(path)
    rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, 1)).reduceByKey((a, b) => a + b).saveAsObjectFile(path)
  }

  def rddPersistence() = {
    val rdd = createWeatherRDD()
    // Cache here
    val cacheRDD = rdd.map(v => (LocalDate.parse(v.getString(0)).getYear, 1)).cache()

    cacheRDD.groupByKey().take(1)
    cacheRDD.reduceByKey((a, b) => a + b).collect()
  }

  def accumulator() = {
    val accumulatorSum = spark.sparkContext.longAccumulator("sum");
    val accumulatorCount = spark.sparkContext.longAccumulator("count");
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7))
    rdd.foreach(x => accumulatorSum.add(x))
    rdd.map(x => accumulatorCount.add(1)).collect()
    println(accumulatorSum)
    println(accumulatorCount)
  }

  def broadcast() = {
    val broadcastVar = spark.sparkContext.broadcast(Array(1, 2, 3))
    spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7)).map(x => println(broadcastVar.value.isEmpty)).collect()
  }

  def main(args: Array[String]) {
    broadcast()
    Thread.sleep(100000)
  }
}

