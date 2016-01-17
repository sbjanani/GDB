import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.Queue

/**
 * Created by jbalaji on 1/4/16.
 */

object GDB {

  def mapFunction(vid:List[Queue[Int]]):List[Queue[Int]] = {

  vid.foreach(item=>item.enqueue(40))

    vid

  }
  def main(args:Array[String]): Unit ={

    val conf:SparkConf = new SparkConf()
      .setAppName("GDB")
      .setMaster("local[2]")
      .setExecutorEnv("--driver-memory", "4g")
      .setExecutorEnv("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val s1 = sc.parallelize(1 to 10,2)
    val s2 = 1 to 5
    sc.broadcast(s2)

    val q1 = Queue[Int]()
    val q2 = Queue[Int]()

    q1.enqueue(1)
    q1.enqueue(2)

    q2.enqueue(3)
    q2.enqueue(4)

    val s3 = List(q1,q2)

    s1.map(x=>{
     mapFunction(s3)
    }).foreach(println)


  }
}