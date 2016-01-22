
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue

/**
 * Created by jbalaji on 1/22/16.
 * This script executes the path query
 */


object PathQuery_nots{



  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val query = List((6,"o",7),(6,"o",1),(0,"o",4))

    // map containing node type
    val nodeTypeMap:Map[Byte,Iterable[Int]] = sc.textFile(Constants.NodeFilePath).map(line=>{
      val parts = line.split("\t")
      (parts(1).toByte,parts.head.toInt)
    }).groupByKey().collectAsMap()

    // index - Each entry is a byte array
    val index = sc.textFile(Constants.IndexFilePath).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    })




    val topology= sc.textFile(Constants.TopologyFilePath).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    })

   val joinedRDD :RDD[(Int,(Array[Byte],Array[Int]))] = index.join(topology)
     .partitionBy(new HashPartitioner(2))


    joinedRDD.persist()


    val seedVertex = nodeTypeMap.getOrElse(query.head._1.toByte,List()).toList.map(x=>(x,Queue[Int]()))

    var vList = sc.parallelize(seedVertex).partitionBy(new HashPartitioner(2))

     println("Seed vertices = "+seedVertex)

    for(queryIndex <- 0 to query.length-2){

      val currentQuery = query(queryIndex)


      def nextVertexList(topologyIter:Iterator[(Int,(Array[Byte],Array[Int]))],vertexIter:Iterator[(Int,Queue[Int])]):Iterator[(Int,Queue[Int])] ={


        //given a vertex and the current query, it determines if the vertex is part of the path or not
        def filterVertex(indexArray:Array[_<:Byte]):Boolean={

          if(indexArray.length>0){
            if(currentQuery._2.equalsIgnoreCase("o")){
              val i = (4*currentQuery._3)+2
              if(indexArray(i)==0 && indexArray(i+1)==0)
                false
              else
                true
            }
            else if(currentQuery._2.equalsIgnoreCase("i")){
              val i = (4*currentQuery._3)+4
              if(indexArray(i)==0 && indexArray(i+1)==0)
                false
              else
                true
            }
            else{
              val i = (4*currentQuery._3)+2
              if(indexArray(i)==0 && indexArray(i+1)==0 && indexArray(i+2)==0 && indexArray(i+3)==0)
                false
              else
                true
            }
          }
          else
            false

        }


        // get the neighbors for the current vertex
        def getNeighbors(neighborList:Array[_<:Int],start:Int,count:Int):Iterator[Int]={

          for(i<-(start to start+count-1).toIterator) yield {
            // println("neighbor i= "+neighborList(i))
            neighborList(i)
          }

        }

        def getStartAndCount(indexArray:Array[_<:Byte], edgeType:Byte, direction:String):(Int,Int)={

          if(direction.equalsIgnoreCase("o")){
            val end = 4*currentQuery._3+2

            val sum = (for(i <- List.range(2,end-1,2)) yield {
              ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))
          }
          else if(direction.equalsIgnoreCase("i")){
            val end = 4*currentQuery._3 + 4
            val sum = (for(i <- List.range(2,end-1,2)) yield { ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))
          }
          else{
            val end = 4*currentQuery._3+2
            val sum = (for(i <- List.range(2,end-1,2)) yield { ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,(((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))+(((indexArray(end+2)<<8)&0x0000FFFF) | (indexArray(end+3)&0x000000FF)))
          }
        }

        vertexIter.map(vertex=>{


          val indexTopologyArray = topologyIter.toMap.getOrElse(vertex._1,(Array(),Array()))
          if(filterVertex(indexTopologyArray._1)){
            val (start,count) = getStartAndCount(indexTopologyArray._1,currentQuery._3.toByte,currentQuery._2)
            getNeighbors(indexTopologyArray._2,start,count)
              .filterNot(vertex._2.contains(_))
              .flatMap(x=>Iterator((x,vertex._2.enqueue(vertex._1))))
          }
           else
            Iterator()


        }).filterNot(_==Iterator()).flatten



      }

      val result = joinedRDD.zipPartitions(vList)(nextVertexList).filter(_._2.size>queryIndex).collect()
      vList = sc.parallelize(result).partitionBy(new HashPartitioner(2))

       //println("Iteration "+queryIndex+" result = "+vList.foreach(x=>println(x._1+"->"+x._2)))

    }


    vList.foreach(x=>println(x._2.enqueue(x._1)))
 }


}