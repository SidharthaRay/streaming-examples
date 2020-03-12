package com.dsm.streaming.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Crime Data Stream").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val inputDf = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "demo")
      .option("startingOffsets", "earliest")
      .load()

    val consoleOutput = inputDf
      .selectExpr("CAST(value AS STRING)")
      .withColumn("value", split($"value", " "))
      .withColumn("value", explode($"value"))
      .groupBy("value").agg("value" -> "count")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }
}
