import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import _root_.util.Constants

import scala.collection.immutable.Queue


/**
 * This file computes path queries on graphx - pregel
 * Created by jbalaji on 1/22/16.
 */

object PageRank_GraphX {


  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)



    val vertices: RDD[(VertexId, Byte)] = sc.textFile(Constants.NodeFilePath)
      .map(line => {
        val parts = line.split("\t")
        if (parts.length == 2)
          (parts(0).toLong, parts(1).toByte)
        else
          null
      }).filter(_ != null)

    val edges: RDD[Edge[Byte]] = sc.textFile(Constants.EdgeFilePath)
      .map(line => {
        val parts = line.split("\t")
        if (parts.length == 3)
          Edge(parts(0).toLong, parts(1).toLong, parts(2).toByte)
        else
          null
      }).filter(_ != null)

    val graph: Graph[Byte, Byte] = Graph(vertices, edges)


   val prGraph = graph.pageRank(0.001)

    prGraph.vertices.foreach(x=>println(x._1+" "+x._2))
  }
}