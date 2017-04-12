package org.sapient.spark

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Pavithran on 09/04/17.
  */

case class Person(id: Int, firstName: String, lastName: String, eMail: String, gender: String, ipAddress: String) {

}

object JsonStreamingDemo {

  def main(args: Array[String]): Unit = {

    println("Hello World!")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Spark-Streaming").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)


    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(5))
    val inputDStream = sparkStreamingContext.socketTextStream("localhost", 9999)

    val personDStream = inputDStream.map {
      jsonString =>
        val gson = new Gson()
        gson.fromJson(jsonString, classOf[Person])
    }

    personDStream.foreachRDD {
      personRDD =>
        personRDD.foreach {
          person =>
            println(person)
        }
    }

    personDStream.print()

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}