import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue

/**
 * Created by jbalaji on 1/17/16.
 * This script executes the path query
 */


object PageRank{



  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")
/*.set("spark.kyroserializer.buffer.max", "2048")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize","32")*/


    val sc = new SparkContext(conf)

    //val edgeType = 2

    val numOfProcessors = 2

    // index - Each entry is a byte array
    val index = sc.textFile(Constants.IndexFilePath,numOfProcessors).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    }).collectAsMap()


    // map containing node type
    val nodeTypeMap:Map[Byte,Iterable[Int]] = sc.textFile(Constants.NodeFilePath,numOfProcessors).map(line=>{
      val parts = line.split("\t")
      (parts(1).toByte,parts.head.toInt)
    }).groupByKey().collectAsMap()


    val topology= sc.textFile(Constants.TopologyFilePath,numOfProcessors).map(line=>{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    }).partitionBy(new HashPartitioner(numOfProcessors))//.filter(_._1==2)

    topology.persist()

    /*//given a vertex and the current query, it determines if the vertex is part of the path or not
    def filterVertexLocal(vertex:Int):Boolean={
      val vertexArray = index.getOrElse(vertex,Array())
      if(vertexArray.length>0){
          val i = (4*edgeType)+2
          if(vertexArray(i)==0 && vertexArray(i+1)==0)
            false
          else
            true
      }
      else
        false

    }*/

    //val numOfVertices = nodeTypeMap.getOrElse(edgeType.toByte,Iterator()).size
    val numOfVertices = index.size

    val seedPR:Double = (1.toDouble/numOfVertices.toDouble)

    var pageRankVector:Array[(Int,Double)] = index
      //.filter(x=>filterVertexLocal(x._1))
      .toArray
      .map(x=>(x._1,seedPR))

   // var pageRankVector:Array[(Int,Double)] = for(x <- index.toArray) yield {(x._1,seedPR)}

    println("initial pr")
    pageRankVector.foreach(x=>println(x._1+" "+x._2))


    // println("Seed vertices = "+seedVertex)

    var delta = Double.MaxValue
    var iteration = 0
    val broadCastIndex = sc.broadcast(index)

    while(delta > 0.001){


      val broadCastPageRank = sc.broadcast(pageRankVector.toMap)
     // val eType = edgeType

      /*//given a vertex and the current query, it determines if the vertex is part of the path or not
      def filterVertex(vertex:Int):Boolean={
        // println("In filter vertex map : vertex "+vertex)
        //println("next query ="+currentQuery)
        val vertexArray = broadCastIndex.value.getOrElse(vertex,Array())
        if(vertexArray.length>0){
          val i = (4*eType)+2
          if(vertexArray(i)==0 && vertexArray(i+1)==0)
            false
          else
            true
        }
        else
          false

      }*/

      def nextVertexList(topologyIter:Iterator[(Int,Array[Int])]):Iterator[(Int,Double)] ={

        // get the neighbors for the current vertex
        def computePR(vert:Int,neighborList:Array[Int],edgeType:Byte,start:Int,count:Int):Double={

          println(" start ="+start+" count="+count+" nLIst="+neighborList.mkString(" "))
          val prSum = for(i<-(start to start+count-1).toList) yield {
            println("neighbor i= "+neighborList(i))
            val outGoingCount:Double = (for(j<- (0 to Constants.NumberOfEdgeTypes-1).toList) yield {
              getStartAndCount(neighborList(i),j.toByte,"o")._2
            }).sum

            println("vertex +"+vert+" number of outgoing neighbors of neighbor node "+neighborList(i) +" is "+outGoingCount)
            val pr:Double = broadCastPageRank.value(neighborList(i))

            pr/outGoingCount
          }
            prSum.sum
        }



        def getStartAndCount(vertex:Int, edgeType:Byte, direction:String):(Int,Int)={

          if(vertex==7){
            println("here")
          }
          val indexArray = broadCastIndex.value(vertex)
          if(direction.equalsIgnoreCase("o")){
            val end = 4*edgeType+2

            val sum = (for(i <- List.range(2,end-1,2)) yield {
              ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))
          }
          else if(direction.equalsIgnoreCase("i")){
            val end = 4*edgeType + 4
            val sum = (for(i <- List.range(2,end-1,2)) yield { ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))
          }
          else{
            val end = 4*edgeType+2
            val sum = (for(i <- List.range(2,end-1,2)) yield { ((indexArray(i)<<8)&0x0000FFFF) | (indexArray(i+1)&0x000000FF)}).sum
            (sum,(((indexArray(end)<<8)&0x0000FFFF) | (indexArray(end+1)&0x000000FF))+(((indexArray(end+2)<<8)&0x0000FFFF) | (indexArray(end+3)&0x000000FF)))
          }
        }

        /*vertexIter.map(vertex=>{

          // println("Vertex = "+vertex)
          val (start,count) = getStartAndCount(vertex._1,eType.toByte,"i")
          // println("start="+start+" count="+count)
          (vertex._1,vertex._2+(0.85*computePR(vertex._1,eType.toByte,start,count)))

        })*/

        topologyIter.map(vertex=>{
          println("Vertex "+vertex._1)
          val neighborPR = for(i<-(0 to Constants.NumberOfEdgeTypes-1).toList)yield{
            val (start,count) = getStartAndCount(vertex._1,i.toByte,"i")
            println("start and count for vertex "+vertex._1+" for edge type "+i+" is "+start+" "+count)
            val pr = computePR(vertex._1,vertex._2,i.toByte,start,count)
            println("pr for edge type "+i+" of vertex "+vertex._1+" is "+pr)
            pr
          }
          (vertex._1,0.15+(0.85*neighborPR.sum))
        })

      }

      val pageRankVector_temp = topology.mapPartitions(nextVertexList,true).collect()

      println("iteration "+iteration+" page rank="+pageRankVector_temp.foreach(x=>println(x._1+"->"+x._2)))

      iteration += 1

      delta = pageRankVector.zip(pageRankVector_temp).map(x=>Math.abs(x._1._2-x._2._2)).sum

      pageRankVector = pageRankVector_temp
    }


    pageRankVector.foreach(x=>println(x._1+"  "+x._2+" iter "+iteration))
  }


}