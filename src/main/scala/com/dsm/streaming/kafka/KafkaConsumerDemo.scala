package com.dsm.streaming.kafka

import org.apache.spark.sql.SparkSession

object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Crime Data Stream").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val inputDf = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  //.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "demo")                          //.option("subscribe", "topic1,topic2")
//      .option("startingOffsets", "earliest")
      .load()

    val consoleOutput = inputDf
      .selectExpr("CAST(value AS STRING)")   // .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
  }
}
