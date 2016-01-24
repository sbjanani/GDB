import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue

/**
 * Created by jbalaji on 1/17/16.
 * This script executes the path query
 */


object PathQuery{



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

    }).collectAsMap()



    val topology= sc.textFile(Constants.TopologyFilePath).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    }).partitionBy(new HashPartitioner(2))

    topology.persist()

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

    var vList = sc.parallelize(seedVertex).partitionBy(new HashPartitioner(2))

   // println("Seed vertices = "+seedVertex)

   for(queryIndex <- 0 to query.length-2){

     val broadCastIndex = sc.broadcast(index)

     val currentQuery = query(queryIndex)
     val nextQuery = query(queryIndex+1)

     //given a vertex and the current query, it determines if the vertex is part of the path or not
     def filterVertex(vertex:Int,currentQuery:(Int,String,Int)):Boolean={
      // println("In filter vertex map : vertex "+vertex)
       //println("next query ="+currentQuery)
       val vertexArray = broadCastIndex.value.getOrElse(vertex,Array())
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

      def nextVertexList(topologyIter:Iterator[(Int,Array[Int])],vertexIter:Iterator[(Int,Queue[Int])]):Iterator[(Int,Queue[Int])] ={

        // get the neighbors for the current vertex
        def getNeighbors(vertex:Int,start:Int,count:Int):Iterator[Int]={

          val neighborList = topologyIter.toMap.getOrElse(vertex,Array())
          for(i<-(start to start+count-1).toIterator) yield {
           // println("neighbor i= "+neighborList(i))
           neighborList(i)
          }

        }

        def getStartAndCount(vertex:Int, edgeType:Byte, direction:String):(Int,Int)={

          val indexArray = broadCastIndex.value.getOrElse(vertex,Array())

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

          // println("Vertex = "+vertex)
          val (start,count) = getStartAndCount(vertex._1,currentQuery._3.toByte,currentQuery._2)
          // println("start="+start+" count="+count)
          getNeighbors(vertex._1,start,count)
            .filter(filterVertex(_,nextQuery))
            .filterNot(vertex._2.contains(_))
            .flatMap(x=>Iterator((x,vertex._2.enqueue(vertex._1))))
        }).flatten



      }

      val result = topology.zipPartitions(vList)(nextVertexList).filter(_._2.size>queryIndex).collect()
      vList = sc.parallelize(result).partitionBy(new HashPartitioner(2))

     // println("Iteration "+queryIndex+" result = ")
     // vList.foreach(x=>println(x._1+"->"+x._2))
    }


    vList.foreach(x=>println(x._2.enqueue(x._1)))
  }


}