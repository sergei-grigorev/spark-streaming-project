package com.griddynamics.stopbot.spark

import com.griddynamics.stopbot.model.EventStructType
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object StructureStreaming2 extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val windowSec = appConf.getDuration("spark.window").getSeconds
  val slideSec = appConf.getDuration("spark.slide").getSeconds
  val watermark = appConf.getDuration("spark.watermark").getSeconds
  val maxEvents = appConf.getLong("app.max-events")
  val minEvents = appConf.getLong("app.min-events")
  val minRate = appConf.getLong("app.min-rate")

  val spark = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .master("local[*]")
    .appName("SparkStructureStreaming")
    .getOrCreate()

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
      from_json(col("value").cast(StringType), schema = EventStructType.schema).alias("value")
    )

  val aggregated =
    parsed
      .withColumn("eventTime", col("value.unix_time").cast(TimestampType))
      .withColumn("clicks", when(col("value.type") === "click", 1).otherwise(0))
      .withColumn("watches", when(col("value.type") === "watch", 1).otherwise(0))
      .drop(col("value"))
      .withWatermark("eventTime", s"$watermark seconds")
      .groupBy(
        col("ip"),
        window(col("eventTime"), s"$windowSec seconds", s"$slideSec seconds"))
      .agg(
        sum("clicks").as("clicks"),
        sum("watches").as("watches"),
        min("eventTime").as("firstEvent"),
        max("eventTime").as("lastEvent")
      )

  val filtered =
    aggregated
      .withColumn("total_events", col("clicks") + col("watches"))
      .filter(col("total_events") > minEvents)
      .withColumn(
        "rate",
        when(col("clicks") > 0, col("watches") / col("clicks"))
          .otherwise(col("watches")))
      .withColumn(
        "incident",
        when(
          col("total_events") > maxEvents,
          concat(
            lit("too much events: "),
            col("total_events"),
            lit(" from "),
            col("firstEvent"),
            lit(" to "),
            col("lastEvent"))
        ).when(
          col("rate") < minRate,
          concat(
            lit("too small rate: "),
            col("rate"),
            lit(" from "),
            col("firstEvent"),
            lit(" to "),
            col("lastEvent"))
        ).otherwise(null))
      .filter(col("incident").isNotNull)
      .select(col("ip"), col("window"), col("incident"))

  val output =
    filtered
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

  /* identify execution plan */
  logger.info(filtered.queryExecution.logical.toString())
  filtered.explain()

  output.awaitTermination()
}
