package org.sapient.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by Pavithran on 17/04/17.
  */
object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {
    /*import org.apache.spark.sql.functions._*/
    import org.apache.spark.sql.SparkSession

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    // Split the lines into words
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()


    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
