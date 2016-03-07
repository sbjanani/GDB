
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue
import scala.util.Random

/**
 * Created by jbalaji on 1/22/16.
 * This script executes the path query
 */


object PathQuery_nots{



  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB_NoTS")
      /*.setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")*/
      .set("spark.kyroserializer.buffer.max", "2048")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize","32")


    val sc = new SparkContext(conf)

    // val qLength = args(0).toInt

    /*val query:List[(Int,String,Int)] = for(i<- List.range(0,qLength)) yield {
       (Random.nextInt(Constants.NumberOfNodeTypes),"o",Random.nextInt(Constants.NumberOfEdgeTypes))
     }*/

    val query = List((2,"o",1),(4,"o",4),(6,"o",1),(0,"o",5))//,(3,"o",7),(4,"0",2))

    //val numOfProcessors = 10
     val numOfProcessors = args(0).toInt

    // map containing node type
    val nodeTypeMap:Map[Byte,Iterable[Int]] = sc.textFile(Constants.NodeFilePath,numOfProcessors).map(line=>{
      val parts = line.split("\t")
      (parts(1).toByte,parts.head.toInt)
    })
      .groupByKey(numOfProcessors).collectAsMap()

    // index - Each entry is a byte array
    val index = sc.textFile(Constants.IndexFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    },true).partitionBy(new HashPartitioner(numOfProcessors))


    val topology= sc.textFile(Constants.TopologyFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    },true).partitionBy(new HashPartitioner(numOfProcessors))

   val joinedRDD :RDD[(Int,(Array[Byte],Array[Int]))] = index.join(topology)
     .partitionBy(new HashPartitioner(numOfProcessors))

    joinedRDD.persist()


    val seedVertex = nodeTypeMap.getOrElse(query.head._1.toByte,List()).toList.map(x=>(x,Queue[Int]()))

    var vList = sc.parallelize(seedVertex).partitionBy(new HashPartitioner(numOfProcessors))

     //("Seed vertices = "+seedVertex)

    for(queryIndex <- 0 to query.length-1 if !vList.isEmpty()){

      val currentQuery = query(queryIndex)


      def nextVertexList(topologyIter:Iterator[(Int,((Array[Byte],Array[Int]),Queue[Int]))]):Iterator[(Int,Queue[Int])] ={


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

        topologyIter.map(vertex=>{


          val indexTopologyArray = vertex._2._1
          if(filterVertex(indexTopologyArray._1)){
            val (start,count) = getStartAndCount(indexTopologyArray._1,currentQuery._3.toByte,currentQuery._2)
            getNeighbors(indexTopologyArray._2,start,count)
              .filterNot(vertex._2._2.contains(_))
              .flatMap(x=>Iterator((x,vertex._2._2.enqueue(vertex._1))))
              .filterNot(_._2.size < queryIndex)
          }
           else
            Iterator()


        }).filterNot(_==Iterator()).flatten

      }

      val result = joinedRDD.join(vList).mapPartitions(nextVertexList,true)
      vList = result.partitionBy(new HashPartitioner(numOfProcessors))

       //println("Iteration "+queryIndex+" result = "+vList.foreach(x=>println(x._1+"->"+x._2)))

    }

    //println("******Result set size ="+vList.collect.size )
 }


}