import java.io.IOException


import org.apache.spark.{SparkContext, SparkConf}

import util.Constants

import scala.reflect.io.Path
import scala.util.Random

/**
 * Created by jbalaji on 1/10/16.
 * This script prepares the Soc-Live Journal dataset for database creation
 */

object DataPrep{


   def main (args: Array[String]){

     val conf:SparkConf = new SparkConf()
       .setAppName("GDB")
       .setMaster("local[2]")
       .setExecutorEnv("--driver-memory", "4g")
       .setExecutorEnv("spark.executor.memory", "2g")

     val sc = new SparkContext(conf)

     val nodePath = Path(Constants.NodeFilePath)
     val edgePath = Path(Constants.EdgeFilePath)

     try {
       nodePath.deleteRecursively()
       edgePath.deleteRecursively()
     } catch {
       case e: IOException => // some file could not be deleted
     }

     val edgeRDD = sc.textFile(Constants.InputFilePath)
       .filter(!_.startsWith("#"))
       .map(_+"\t"+Random.nextInt(8))

     edgeRDD.saveAsTextFile(Constants.EdgeFilePath)


   edgeRDD.flatMap(line=>{

         val parts = line.split("\t")
         if(parts.length==3)
           List(parts.head,parts(1))

         else
           None
       })
       .distinct()
       .map(_+"\t"+Random.nextInt(8)).saveAsTextFile(Constants.NodeFilePath)





  }
}