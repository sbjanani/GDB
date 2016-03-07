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
    /*  .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")*/
    .set("spark.kyroserializer.buffer.max", "2048")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize","32")

    val sc = new SparkContext(conf)

    val query = List((2,"o",1),(4,"o",2),(3,"o",2),(5,"o",2),(2,"o",6))
    // This RDD is a map from node id to node type Map(nodeID->nodeType)
    val nodeMapRDD:Map[Int,Byte] = sc.wholeTextFiles(Constants.NodeFilePath,10).flatMap(x=>{
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

    val graphRDD:RDD[(Int,(Byte,Map[String,Map[Byte,List[Int]]]))] = sc.wholeTextFiles(Constants.EdgeFilePath,10).flatMap(x=>{

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
    }).groupByKey(10).mapPartitions(for(node<-_) yield{


      val nodeID = node._1.toInt
      val outInGroup: Map[String, Map[Byte, List[Int]]] = node._2.map(outIn => (outIn._1, (outIn._2, outIn._3)))
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).toList)
        .mapValues(y => y.map(f => (f._2.toByte, f._1))
          .groupBy(_._1)
          .mapValues(y => y.map(_._2.toInt)))

      (nodeID,(nodeMapRDD.getOrElse(nodeID,-1),outInGroup))
    },true)

    graphRDD.persist()


    val nodeTypeMap:Map[Byte,Iterable[Int]] = nodeMapRDD.map(x=>(x._2,x._1)).groupBy(_._1).mapValues(x=>x.map(_._2))


    val seedVertex = nodeTypeMap.getOrElse(query.head._1.toByte,List()).toList.map(x=>(x,Queue[Int]()))

    var vList = sc.parallelize(seedVertex).partitionBy(new HashPartitioner(10))

   //println("Seed vertices = "+seedVertex)

    for(queryIndex <- 0 to query.length-2 if !vList.isEmpty()){

      val currentQuery = query(queryIndex)


      def nextVertexList(topologyIter:Iterator[(Int,(Byte,Map[String,Map[Byte,List[Int]]]))],vertexIter:Iterator[(Int,Queue[Int])]):Iterator[(Int,Queue[Int])] ={


        // get the neighbors for the current vertex
        def getNeighbors(record:(Byte,Map[String,Map[Byte,List[Int]]]),currentQuery:(Int,String,Int)):Iterator[Int]={

          if(record._1==currentQuery._1)
            record._2.getOrElse(currentQuery._2,Map()).getOrElse(currentQuery._3.toByte,List()).toIterator
          else
            Iterator()

        }

        val topologyMap = topologyIter.toMap

        vertexIter.map(vertex=>{

          val graphRecord = topologyMap.getOrElse(vertex._1,null)
            getNeighbors(graphRecord,currentQuery)
              .filterNot(vertex._2.contains(_))
              .flatMap(x=>Iterator((x,vertex._2.enqueue(vertex._1))))

        }).filterNot(_==Iterator()).flatten



      }

      val result = graphRDD.zipPartitions(vList)(nextVertexList).filter(_._2.size>queryIndex)
      vList = result.partitionBy(new HashPartitioner(10))

      //println("Iteration "+queryIndex+" result = "+vList.foreach(x=>println(x._1+"->"+x._2)))

    }


    println("******Result set size ="+vList.collect.size )
      //vList.foreach(x=>println(x._2.enqueue(x._1)))


  }
}
