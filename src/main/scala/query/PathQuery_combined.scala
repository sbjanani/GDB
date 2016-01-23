import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants

import scala.collection.Map
import scala.collection.immutable.Queue

/**
 * Created by jbalaji on 1/23/16.
 */

object PathQuery_combined{
  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val query = List((6, "o", 7), (6, "o", 1), (0, "o", 2),(2,"o",0),(7,"o",1))
    // This RDD is a map from node id to node type Map(nodeID->nodeType)
    val nodeMapRDD:Map[Int,Byte] = sc.wholeTextFiles(Constants.NodeFilePath).flatMap(x=>{
      if(x._2.length>0){
        x._2.split("\n").flatMap(entry=>{
          val parts = entry.split("\t")
          if(parts.length==2)
            Map(parts.head.toInt->parts(1).toByte)
          else
            None
        })
      }
      else
        None
    }).collectAsMap()

    val graphRDD:RDD[(Int,(Byte,Map[String,Map[Byte,List[Int]]]))] = sc.wholeTextFiles(Constants.EdgeFilePath).flatMap(x=>{

      if(x._2.length>0){
        x._2.split("\n").flatMap(entry=>{
          val parts = entry.split("\t")
          if(parts.length==3)
            List((parts.head,("o",parts(1),parts(2))),(parts(1),("i",parts.head,parts(2))))
          else
            None
        })
      }
      else
        None
    }).groupByKey().map(node=> {


      val nodeID = node._1.toInt
      val outInGroup: Map[String, Map[Byte, List[Int]]] = node._2.map(outIn => (outIn._1, (outIn._2, outIn._3)))
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).toList)
        .mapValues(y => y.map(f => (f._2.toByte, f._1))
          .groupBy(_._1)
          .mapValues(y => y.map(_._2.toInt)))

      (nodeID,(nodeMapRDD.getOrElse(nodeID,-1),outInGroup))
    })

    graphRDD.persist()

    val nodeTypeMap:Map[Byte,Iterable[Int]] = nodeMapRDD.map(x=>(x._2,x._1)).groupBy(_._1).mapValues(x=>x.map(_._2))


    val seedVertex = nodeTypeMap.getOrElse(query.head._1.toByte,List()).toList.map(x=>(x,Queue[Int]()))

    var vList = sc.parallelize(seedVertex).partitionBy(new HashPartitioner(2))

    println("Seed vertices = "+seedVertex)

    for(queryIndex <- 0 to query.length-2){

      val currentQuery = query(queryIndex)


      def nextVertexList(topologyIter:Iterator[(Int,(Byte,Map[String,Map[Byte,List[Int]]]))],vertexIter:Iterator[(Int,Queue[Int])]):Iterator[(Int,Queue[Int])] ={


        // get the neighbors for the current vertex
        def getNeighbors(record:(Byte,Map[String,Map[Byte,List[Int]]])):Iterator[Int]={

          if(record._1==currentQuery._1)
            record._2.getOrElse(currentQuery._2,Map()).getOrElse(currentQuery._3.toByte,List()).toIterator
          else
            Iterator()

        }

        vertexIter.map(vertex=>{
          val graphRecord = topologyIter.toMap.getOrElse(vertex._1,null)
            getNeighbors(graphRecord)
              .filterNot(vertex._2.contains(_))
              .flatMap(x=>Iterator((x,vertex._2.enqueue(vertex._1))))

        }).filterNot(_==Iterator()).flatten



      }

      val result = graphRDD.zipPartitions(vList)(nextVertexList).filter(_._2.size>queryIndex).collect()
      vList = sc.parallelize(result).partitionBy(new HashPartitioner(2))

      //println("Iteration "+queryIndex+" result = "+vList.foreach(x=>println(x._1+"->"+x._2)))

    }


    vList.foreach(x=>println(x._2.enqueue(x._1)))


  }
}
