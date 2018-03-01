package com.griddynamics.stopbot.spark.stream

import com.griddynamics.stopbot.implicits._
import com.griddynamics.stopbot.model.MessageStructType
import com.griddynamics.stopbot.spark.logic.StructureWindowPlain
import com.griddynamics.stopbot.spark.stream.KafkaDataSetToConsole.appConf
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, TimestampType}

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object KafkaStructureToParquet extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val debug = appConf.getBoolean("debug-mode")

  val sparkBuilder = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .appName(appConf.getString("app.name"))

  val spark =
    if (debug) {
      sparkBuilder
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
        .master("local[2]")
        .getOrCreate()
    } else {
      sparkBuilder
        .getOrCreate()
    }

  /* kafka streaming */
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", appConf.getString("kafka.brokers"))
    .option("subscribe", appConf.getStringList("kafka.topic").asScala.mkString(","))
    .option("startingOffsets", appConf.getString("kafka.offset.reset"))
    .load()

  /* key = user_ip, value = message json */
  val parsed =
    df.select(
      col("key").cast(StringType).as("ip"),
      from_json(col("value").cast(StringType), schema = MessageStructType.schema).alias("value")
    )
      .withColumn("eventTime", col("value.unix_time").cast(TimestampType))
      .selectExpr("ip", "value.type as action", "eventTime")

  /* run structure streaming with custom UDF */
  val filtered = StructureWindowPlain
    .findIncidents(
      input = parsed,
      window = appConf.getDuration("spark.window"),
      slide = appConf.getDuration("spark.slide"),
      watermark = appConf.getDuration("spark.watermark"),
      minEvents = appConf.getLong("app.min-events"),
      maxEvents = appConf.getLong("app.max-events"),
      minRate = appConf.getDouble("app.min-rate")
    )

  val parquet =
    filtered
      .writeStream
      .outputMode(OutputMode.Append())
      .partitionBy("ip")
      .format("parquet")
      .option("path", "/tmp/parquet")
      .start()

  parquet.explain(true)

  parquet.awaitTermination()
}
