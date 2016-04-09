/**
 * Created by jbalaji on 3/17/16.
 * This script executes the pathquery for the full graph without the ts segment but with a pre computed index structure.
 * This index structure is query independent
 */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue
import scala.util.Random



object PathQuery_Index{



  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName("GDB_Index")
      .set("spark.kyroserializer.buffer.max", "2048")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize","32")
     /*.setMaster("local[2]")
    .setExecutorEnv("--driver-memory", "4g")
    .setExecutorEnv("spark.executor.memory", "2g")*/

    val sc = new SparkContext(conf)

    val query = List((2.toByte,"o",1.toByte),(4.toByte,"o",4.toByte),(6.toByte,"o",5.toByte),(3.toByte,"o",7.toByte),(5.toByte,"o",6.toByte))//,(1.toByte,"o",1.toByte))

    val numOfProcessors = args(0).toInt



    val structure = sc.textFile(Constants.IndexFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    },true).partitionBy(new HashPartitioner(numOfProcessors))



   val edgeIndex:RDD[((Byte,String,Byte),Int)] = structure.flatMap(x=>{
     val vertex = x._1
     val sArray = x._2
     sArray.drop(2).grouped(4).zipWithIndex.flatMap(eType=>{

      // println("array = "+eType._1.mkString(" ")+" type = "+eType._2.toByte)
       val edgeType = eType._2.toByte

       val outgoing = if(!((eType._1(0) == 0) && (eType._1(1) == 0))) ((sArray(1),"o",edgeType),vertex) else null

       val incoming = if(!((eType._1(2) == 0) && (eType._1(3) == 0))) ((sArray(1),"i",edgeType),vertex) else null

       List(outgoing,incoming).filterNot(_==null)

     })
   }).persist()




   /* val vertexIndex:RDD[(Byte,List[Int])] = structure.map(x=>{
      val vertex = x._1
      val sArray = x._2

      (sArray(1),vertex)
    }).combineByKey(
      (x:Int) => List(x),
      (acc:List[Int],x) => x::acc,
      (acc1:List[Int],acc2:List[Int]) => acc1 ::: acc2
    )*/

    val topology= sc.textFile(Constants.TopologyFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    },true).partitionBy(new HashPartitioner(numOfProcessors))

    val graph:RDD[(Int,(Array[Byte],Array[Int]))] = structure.join(topology,new HashPartitioner(numOfProcessors)).persist()


    var vList:RDD[(Int,Queue[Int])]=  edgeIndex.filter(_._1==query.head).map(x=>(x._2,Queue[Int]())).partitionBy(new HashPartitioner(numOfProcessors))


    for(queryIndex <- 0 to query.length-2 if !vList.isEmpty()){


      val currentQuery = query(queryIndex)


      def nextVertexList(topologyIter:Iterator[(Int,((Array[Byte],Array[Int]),Queue[Int]))]):Iterator[(Int,Queue[Int])] ={

        // get the neighbors for the current vertex
        def getNeighbors(neighborList:Array[Int],start:Int,count:Int,currentQueue:Queue[Int]):Iterator[Int]={

          for(i<-(start to start+count-1).toIterator) yield {
            // println("neighbor i= "+neighborList(i))
            // if(filterVertex(indexMap.get(neighborList(i)).get,nextQuery) && !currentQueue.contains(neighborList(i)))
            neighborList(i)
            //else
            //-1
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

          val (start,count) = getStartAndCount(vertex._2._1._1,currentQuery._3.toByte,currentQuery._2)
          //println("start="+start+" count="+count)
          getNeighbors(vertex._2._1._2,start,count,vertex._2._2)
            .filterNot(vertex._2._2.contains(_))
            .flatMap(x=>Iterator((x,vertex._2._2.enqueue(vertex._1))))
            .filter(_._2.size>queryIndex)
        }).flatten


      }


      val result:RDD[(Int,Queue[Int])] = graph.join(vList,new HashPartitioner(numOfProcessors)).mapPartitions(nextVertexList)

      val nextV:RDD[(Int,Int)] =  edgeIndex.filter(_._1==query(queryIndex+1)).map(x=>(x._2,x._2)).partitionBy(new HashPartitioner(numOfProcessors))

      val partResult = result.partitionBy(new HashPartitioner(numOfProcessors))

      val p = nextV.join(partResult).mapValues(_._2).partitionBy(new HashPartitioner(numOfProcessors))

    // println("***************************\niter "+queryIndex+ " size ="+p.collect().size+"*****************************")

      vList.unpersist()
      vList = p


    }

   // println("********** \nResult size = "+vList.collect().foreach(println)+"\n*****************")

  }


}