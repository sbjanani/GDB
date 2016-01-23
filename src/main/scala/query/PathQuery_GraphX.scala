import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import _root_.util.Constants

import scala.collection.immutable.Queue


/**
 * Created by jbalaji on 1/22/16.
 */

object PathQuery_GraphX {


  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val query = List((6, "o", 7), (6, "o", 1), (0, "o", 2),(2,"o",0),(7,"o",1))

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


    // create new graph
    val newGraph: Graph[(Byte, List[Queue[Long]], Int), Byte] = graph.mapVertices { (vId, vType) => {
     //println("vid ="+vId+" vType= "+vType+" "+(vType, List(Queue(Long.MinValue)), 0).toString())
      (vType, List(Queue(Long.MinValue)), 0)
    } }

    //newGraph.vertices.collect.foreach(x=>println(x._1+" type ="+x._2._1.toByte+" "+x._2._2.toString()+" "+x._2._3))

    val initialMessage = (List(Queue(Long.MinValue)), 0)

    def vertexProgram(id: VertexId, attr: (Byte, List[Queue[Long]], Int), msg: (List[Queue[Long]], Int)): (Byte, List[Queue[Long]], Int) = {



      if (attr._1 == query(msg._2)._1) {
        if(msg._2==0){
         // println("id="+id+"first if iter = "+msg._2+" query type= "+query(msg._2)._1+" "+(attr._1, List(Queue(id.toLong)), msg._2).toString())
          (attr._1, List(Queue(id.toLong)), msg._2)
        }

        else{
          val newList = for(q<-msg._1)yield q.enqueue(id.toLong)
          newList.foreach(x=>println(id+" "+x.toString()))
          //println("id="+id+" Not first if iter = "+msg._2+" query type= "+query(msg._2)._1+" "+(attr._1, newList, msg._2).toString())
          (attr._1, newList, msg._2)
        }


      }

      else{
        //println("Not matching id="+id+" iter = "+msg._2+" query type= "+query(msg._2)._1+" "+(attr._1, List(Queue()), msg._2).toString())
        (attr._1, List(Queue()), msg._2)
      }

    }

    def sendMessage(edge: EdgeTriplet[(Byte, List[Queue[Long]], Int), Byte]): Iterator[(VertexId, (List[Queue[Long]], Int))] = {

      if (edge.srcAttr._3<query.size && edge.attr == query(edge.srcAttr._3)._3 && edge.srcAttr._2!=List(Queue())){
        //println("node id"+edge.srcId+" destination="+edge.dstId+" node queue ="+edge.srcAttr._2.toString()+" iteration="+(edge.srcAttr._3+1)+" edge typ="+edge.attr)
        Iterator((edge.dstId, (edge.srcAttr._2, edge.srcAttr._3+1)))
      }

      else
        Iterator.empty
    }

    def combineMessage(msg1:(List[Queue[Long]], Int),msg2:(List[Queue[Long]], Int)):(List[Queue[Long]], Int)={
      //println("Inside combine message")
      (msg1._1 ++ msg2._1,Math.max(msg1._2,msg2._2))
    }

   val res = Pregel(newGraph,initialMessage,query.length-1)(vertexProgram,sendMessage,combineMessage)

    res.vertices.collect
      .filter(_._2._3==query.size-1)
      .foreach(x=>{
      println("Vertex "+x._1)
      x._2._2.foreach(y=> {
        print(y.mkString(" ")+";")
        println
      })
    })
  }
}