import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import util.Constants

import scala.collection.Map

/**
 * Created by jbalaji on 1/11/16.
 */

object LoadData{

  def  main (args: Array[String]){

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

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



    sc.broadcast(nodeMapRDD)

    val graphRDD = sc.wholeTextFiles(Constants.EdgeFilePath).flatMap(x=>{


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
    }).groupByKey().map(node=>{


      val nodeID = node._1.toInt
      val outInGroup:Map[String,Map[String,List[Int]]] = node._2.map(outIn=>(outIn._1,(outIn._2,outIn._3)))
        .groupBy(_._1)
        .mapValues(x=>x.map(_._2).toList)
        .mapValues(y=>y.map(f=>(f._2,f._1))
          .groupBy(_._1)
          .mapValues(y=>y.map(_._2.toInt)))

      val indexArray = new Array[Byte](2+(4*Constants.NumberOfEdgeTypes))
      indexArray(0)=0
      indexArray(1) = nodeMapRDD(nodeID.toInt)

     val neighborList:List[List[Int]] =  for(index <- (2 until indexArray.length-5 by 4).toList ) yield {

        val outList:List[Int] = outInGroup.getOrElse("o",Map()).getOrElse((index/4).toString,List())
          indexArray(index) = (outList.length.toShort >>> 8).toByte
          indexArray(index+1) = outList.length.toShort.toByte


        val inList:List[Int] = outInGroup.getOrElse("i",Map()).getOrElse((index/4).toString,List())
        indexArray(index+2) = (inList.length.toShort >>> 8).toByte
        indexArray(index+3) = inList.length.toShort.toByte


         inList:::outList



        }

      (nodeID,indexArray,neighborList.flatten)
    }).collect().sortBy(_._1)

    val indexFileWriter = new PrintWriter(Constants.IndexFilePath)
    val topologyFileWriter = new PrintWriter(Constants.TopologyFilePath)


    try{
      graphRDD.foreach(x=>{
        topologyFileWriter.println(x._1+";"+x._3.mkString(" "))
        indexFileWriter.print(x._1+";")
        indexFileWriter.print(x._2(0)+" "+x._2(1)+" ")
        for(i<-2 to x._2.length-1 by 2)
          indexFileWriter.print((x._2(i) << 8 | x._2(i+1)).toShort+" ")
        indexFileWriter.println
      })

    }

    finally{
      indexFileWriter.flush()
      topologyFileWriter.flush()
      indexFileWriter.close()
      topologyFileWriter.close()
    }






  }
}