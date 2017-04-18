package org.sapient.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Pavithran on 09/04/17.
  */
object SocketStreamingClient {
  def main(args: Array[String]): Unit = {

    println("Hello World!")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Spark-Streaming").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)

    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(5))
    val lines = sparkStreamingContext.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
