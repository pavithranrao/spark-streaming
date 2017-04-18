package org.sapient.spark

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset

/**
  * Created by Pavithran on 18/04/17.
  */
object StructuredStreamingKafka {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val spark = SparkSession
      .builder
      .appName("Kafka Stream DataSet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val topic = "test"
    val bootStrapServer = "localhost:9092"
    val offset = "latest"

    val personDataSet: Dataset[Person] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", offset)
      .load()
      .select("value")
      .as[String]
      .map {
        x =>
          val gson = new Gson()
          gson.fromJson(x, classOf[Person])
      }


    val query = personDataSet.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
