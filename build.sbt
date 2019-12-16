
lazy val commonSettings = Seq(
  name := "spark-scala",
  version := "1.0",
  scalaVersion := "2.13.1",
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value % "scala-tool",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-hive" % "2.4.4",
  "org.apache.spark" %% "spark-avro" % "2.4.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4",
  "org.apache.spark" %% "spark-graphx" % "2.4.4"
)

