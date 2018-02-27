package com.griddynamics.stopbot.spark.stream

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Structure streaming for the custom source.
  */
object MySourceProviderStreaming extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val windowSec = appConf.getDuration("spark.window").getSeconds
  val slideSec = appConf.getDuration("spark.slide").getSeconds

  val spark = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .master("local[2]")
    .appName("SparkStructureStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
    .getOrCreate()

  /* kafka streaming */
  val df = spark
    .readStream
    .format("com.griddynamics.stopbot.source.MySourceProvider")
    .load()
    .groupBy(col("key"))
    .agg(
      sum("value").as("total"),
      min("value").as("first"),
      max("value").as("last")
    )

  val output =
    df
      .writeStream
      .option("checkpointLocation", "/tmp/spark-checkpoint")
      .outputMode(OutputMode.Update())
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

  output.explain(true)

  output.awaitTermination()
}
