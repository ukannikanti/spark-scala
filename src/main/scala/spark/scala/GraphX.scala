package spark.scala

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.sql.SparkSession

object GraphX {

  val spark = SparkSession.builder().appName("SparkDataset")
    .config("spark.eventLog.enabled", true).master("local[*]")
    .getOrCreate()

  def graphx(): Unit = {
    // Create an RDD for the vertices
    val users: org.apache.spark.rdd.RDD[(VertexId, (String, String))] =
      spark.sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: org.apache.spark.rdd.RDD[Edge[String]] =
      spark.sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph = org.apache.spark.graphx.Graph(users, relationships, defaultUser)

    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)

  }

  def main(args: Array[String]): Unit = {
    graphx()
  }
}
