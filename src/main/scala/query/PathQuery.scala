import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue
import scala.util.Random

/**
 * Created by jbalaji on 1/17/16.
 * This script executes the path query
 */


object PathQuery{



  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName("GDB_PathQuery")
      .set("spark.kyroserializer.buffer.max", "2048")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize","32")

      /*.setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")*/
    val sc = new SparkContext(conf)

    // val qLength = args(0).toInt

    /*val query:List[(Int,String,Int)] = for(i<- List.range(0,qLength)) yield {
       (Random.nextInt(Constants.NumberOfNodeTypes),"o",Random.nextInt(Constants.NumberOfEdgeTypes))
     }*/

    val query = List((2,"o",1),(4,"o",4),(6,"o",1),(0,"o",5))//,(3,"o",7))

    //val numOfProcessors = 10
     val numOfProcessors = args(0).toInt


    // map containing node type
    val nodeTypeMap:Map[Byte,Iterable[Int]] = sc.textFile(Constants.NodeFilePath,numOfProcessors).map(line=>{
      val parts = line.split("\t")
      (parts(1).toByte,parts.head.toInt)
    }).groupByKey(numOfProcessors).collectAsMap()

    // index - Each entry is a byte array
     val index = sc.textFile(Constants.IndexFilePath,numOfProcessors).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    }).collectAsMap()



    val topology= sc.textFile(Constants.TopologyFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    },true).partitionBy(new HashPartitioner(numOfProcessors))



    topology.cache()

    topology.setName("Topology RDD")


    //given a vertex and the current query, it determines if the vertex is part of the path or not
    def filterVertexLocal(vertex:Int,currentQuery:(Int,String,Int)):Boolean={
      val vertexArray = index.getOrElse(vertex,Array())
      if(vertexArray.length>0){
        if(currentQuery._2.equalsIgnoreCase("o")){
          val i = (4*currentQuery._3)+2
          if(vertexArray(i)==0 && vertexArray(i+1)==0)
            false
          else
            true
        }
        else if(currentQuery._2.equalsIgnoreCase("i")){
          val i = (4*currentQuery._3)+4
          if(vertexArray(i)==0 && vertexArray(i+1)==0)
            false
          else
            true
        }
        else{
          val i = (4*currentQuery._3)+2
          if(vertexArray(i)==0 && vertexArray(i+1)==0 && vertexArray(i+2)==0 && vertexArray(i+3)==0)
            false
          else
            true
        }
      }
      else
        false

    }


    val seedVertex = nodeTypeMap.getOrElse(query.head._1.toByte,List()).filter(filterVertexLocal(_,query.head)).toList.map(x=>(x,Queue[Int]()))

    var vList = sc.parallelize(seedVertex,numOfProcessors).partitionBy(new HashPartitioner(numOfProcessors))

    vList.setName("vList init")
    val broadCastIndex = sc.broadcast(index)

   // println("Seed vertices = "+seedVertex)

   for(queryIndex <- 0 to query.length-2 if !vList.isEmpty()){


     val currentQuery = query(queryIndex)
     val nextQuery = query(queryIndex+1)



      def nextVertexList(topologyIter:Iterator[(Int,(Array[Int],Queue[Int]))], broadCastIndex:Broadcast[Map[Int,Array[Byte]]]):Iterator[(Int,Queue[Int])] ={

        val indexMap = broadCastIndex.value


        //given a vertex and the current query, it determines if the vertex is part of the path or not
        def filterVertex(vertexArray:Array[Byte],currentQuery:(Int,String,Int)):Boolean={
          // println("In filter vertex map : vertex "+vertex)
          //println("next query ="+currentQuery)
          //println("Vertex Array ="+vertexArray.mkString(" "))
          if(vertexArray.length>0){
            if(currentQuery._2.equalsIgnoreCase("o")){
              val i = (4*currentQuery._3)+2
              if(vertexArray(i)==0 && vertexArray(i+1)==0)
                false
              else
                true
            }
            else if(currentQuery._2.equalsIgnoreCase("i")){
              val i = (4*currentQuery._3)+4
              if(vertexArray(i)==0 && vertexArray(i+1)==0)
                false
              else
                true
            }
            else{
              val i = (4*currentQuery._3)+2
              if(vertexArray(i)==0 && vertexArray(i+1)==0 && vertexArray(i+2)==0 && vertexArray(i+3)==0)
                false
              else
                true
            }
          }
          else
            false

        }

        // get the neighbors for the current vertex
        def getNeighbors(neighborList:Array[Int],start:Int,count:Int,currentQueue:Queue[Int]):Iterator[Int]={

          for(i<-(start to start+count-1).toIterator) yield {
           // println("neighbor i= "+neighborList(i))
            if(filterVertex(indexMap.get(neighborList(i)).get,nextQuery) && !currentQueue.contains(neighborList(i)))
           neighborList(i)
            else
              -1
          }

        }.filterNot(_== -1)

        def getStartAndCount(indexArray:Array[Byte],edgeType:Byte, direction:String):(Int,Int)={


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

           //println("Vertex = "+vertex+" broadcast ="+broadCastMap.getOrElse(vertex._1,Array()).mkString(" "))

          val (start,count) = getStartAndCount(indexMap.get(vertex._1).get,currentQuery._3.toByte,currentQuery._2)
           //println("start="+start+" count="+count)
          getNeighbors(vertex._2._1,start,count,vertex._2._2)
            .filterNot(vertex._2._2.contains(_))
            .flatMap(x=>Iterator((x,vertex._2._2.enqueue(vertex._1))))
           .filter(_._2.size>queryIndex)
        }).flatten


      }

    // val result = topology.mapPartitions()

     val result = topology.join(vList,new HashPartitioner(numOfProcessors)).mapPartitions(x=>nextVertexList(x,broadCastIndex),true)
     // val result = topology.zipPartitions(vList,true)((x,y)=>nextVertexList(x,y,broadCastIndex))
     vList.unpersist()
      vList = result.partitionBy(new HashPartitioner(numOfProcessors))

     vList.setName("vlist iter "+queryIndex)

     //println("Iteration "+queryIndex+" result = "+vList.collect.foreach(x=>println(x._1+"->"+x._2)))
    }

//println("Result")

   // val count:Map[Int,Int] = vList.map(x=>(x._2.size,1)).reduceByKey(_+_).collectAsMap()
   // println("******Result set size ="+count)
  }


}