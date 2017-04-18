package org.sapient.spark

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{ Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by Pavithran on 09/04/17.
  */


object KafkaStreamingConsumerClient {

  def main(args: Array[String]): Unit = {

    println("Hello World!")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Spark-Streaming").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(5))
    sparkStreamingContext.checkpoint("checkpoint")

    val zkQuorum = "localhost:2181"
    val group = "default-group"
    val topics = "test"
    val numThreads = 1
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topicMap = topics.split(",").map((_, numThreads)).toMap


    //val lines = KafkaUtils.createStream(sparkStreamingContext, zkQuorum, group, topicMap)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sparkStreamingContext, kafkaParams, topicsSet)

    val message = lines.map(_._2)
    //message.print()

    val words = message.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    //wordCounts.print()

    val windowedWordCounts = wordCounts.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
    windowedWordCounts.print()

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}