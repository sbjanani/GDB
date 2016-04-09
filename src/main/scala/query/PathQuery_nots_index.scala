import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants
import scala.collection.Map
import scala.collection.immutable.Queue
import scala.util.Random

/**
 * Created by jbalaji on 1/17/16.
 * This script executes the path query version without the TS region, but with a prune set for each query segment.
 * This is query dependent.
 */


object PathQuery_nots_index{



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

    val query = List((2,"o",1),(4,"o",4),(6,"o",5),(3,"o",7),(5,"o",6))//,(1,"o",1))

    val numOfProcessors = args(0).toInt


    def filterVertexLocal1(vertexArray:Array[Byte], currentQuery:(Int,String,Int)):Boolean={


      if(currentQuery._2.equalsIgnoreCase("o")){
        val i = (4*currentQuery._3)+2
        if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0))
          false
        else
          true
      }
      else if(currentQuery._2.equalsIgnoreCase("i")){
        val i = (4*currentQuery._3)+4
        if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0))
          false
        else
          true
      }
      else{
        val i = (4*currentQuery._3)+2
        if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0 && vertexArray(i+2)==0 && vertexArray(i+3)==0))
          false
        else
          true
      }


    }

    def filterVertexLocal(vertexArray:Array[Byte], currentQuery:(Int,String,Int), prevEdgeType:Int):Boolean={


      // check if it has an incoming vertex of the previous edge type
      val i = (4*prevEdgeType)+4
      if(vertexArray(i)==0 && vertexArray(i+1)==0)
        false

      // check if it has outgoing vertices of the current type
      else{
        if(currentQuery._2.equalsIgnoreCase("o")){
          val i = (4*currentQuery._3)+2
          if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0))
            false
          else
            true
        }
        else if(currentQuery._2.equalsIgnoreCase("i")){
          val i = (4*currentQuery._3)+4
          if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0))
            false
          else
            true
        }
        else{
          val i = (4*currentQuery._3)+2
          if(vertexArray(1)!=currentQuery._1.toByte ||(vertexArray(i)==0 && vertexArray(i+1)==0 && vertexArray(i+2)==0 && vertexArray(i+3)==0))
            false
          else
            true
        }
      }

    }


    val structure = sc.textFile(Constants.IndexFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toByte))

    },true).partitionBy(new HashPartitioner(numOfProcessors))

    // index - Each entry is a byte array
    val index = structure.flatMap(x=>{

      val vertex = x._1
      val array = x._2

      if(array.size > 0) {

        query.zipWithIndex.sliding(2).flatMap(current =>{
          val res1 = if(current.head._2==0) {
            if (filterVertexLocal1(array, current.head._1))
              (current.head._2, vertex)
            else
              null
          }
          else null


          val res2 = if(filterVertexLocal(array,current(1)._1,current.head._1._3))
            (current(1)._2,vertex)
          else
            null

          val result:List[(Int,Int)]=List(res1,res2).filterNot(_==null)

          result

        })
      }
      else
        null


    }).filter(_!=null)

    val topology= sc.textFile(Constants.TopologyFilePath,numOfProcessors).mapPartitions(for(line <- _) yield{
      val parts = line.split(";")
      (parts(0).toInt,parts(1).split(" ").map(_.toInt))
    },true).partitionBy(new HashPartitioner(numOfProcessors))

    val graph:RDD[(Int,(Array[Byte],Array[Int]))] = structure.join(topology,new HashPartitioner(numOfProcessors)).persist()


    var vList = index.filter(_._1==0).map(x=>(x._2,Queue[Int]())).partitionBy(new HashPartitioner(numOfProcessors))


    vList.setName("vList init")

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

      val nextV:RDD[(Int,Int)] = index.filter(_._1==queryIndex+1).map(x=>(x._2,x._2)).partitionBy(new HashPartitioner(numOfProcessors))

      val partResult = result.partitionBy(new HashPartitioner(numOfProcessors))

      val p = nextV.join(partResult).mapValues(_._2).partitionBy(new HashPartitioner(numOfProcessors))

      vList.unpersist()
      vList = p

    }

    // println("********** \nResult size = "+vList.collect().size+"\n*****************")

  }


}