package com.griddynamics.stopbot.spark

import com.griddynamics.stopbot.model.EventStructType
import com.griddynamics.stopbot.sink.CassandraSinkForeach
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object StructureStreaming extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val windowSec = appConf.getDuration("spark.window").getSeconds
  val slideSec = appConf.getDuration("spark.slide").getSeconds
  val watermark = appConf.getDuration("spark.watermark").getSeconds
  val maxEvents = appConf.getLong("app.max-events")
  val minEvents = appConf.getLong("app.min-events")
  val minRate = appConf.getLong("app.min-rate")
  val banTimeSec = appConf.getDuration("app.ban-time").getSeconds
  val banRecordTTL = (banTimeSec / 1000).toInt

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

  /* key = user_ip, value =  */
  val parsed =
    df.select(
      col("key").cast(StringType),
      from_json(col("value").cast(StringType), schema = EventStructType.schema).alias("value")
    )

  val aggregated =
    parsed
      .withColumn("eventTime", col("value.unix_time").cast(TimestampType))
      .selectExpr("key as ip", "value.type as action", "eventTime")
      .withWatermark("eventTime", s"$watermark seconds")
      .groupBy(
        col("ip"),
        window(col("eventTime"), s"$windowSec seconds", s"$slideSec seconds"))
      .agg(EventAggregationUdf(col("action"), col("eventTime")).alias("aggregation"))

  val filtered =
    aggregated
      .withColumn("total_events", col("aggregation.clicks") + col("aggregation.watches"))
      .filter(col("total_events") > minEvents)
      .withColumn(
        "rate",
        when(col("aggregation.clicks") > 0, col("aggregation.watches") / col("aggregation.clicks"))
          .otherwise(col("aggregation.watches")))
      .withColumn(
        "incident",
        when(
          col("total_events") > maxEvents,
          concat(
            lit("too much events: "),
            col("total_events"),
            lit(" from "),
            col("aggregation.firstEvent"),
            lit(" to "),
            col("aggregation.lastEvent"))
        ).when(
          col("rate") < minRate,
          concat(
            lit("too small rate: "),
            col("rate"),
            lit(" from "),
            col("aggregation.firstEvent"),
            lit(" to "),
            col("aggregation.lastEvent"))
        ).otherwise(null))
      .filter(col("incident").isNotNull)
      // (to + banTimeSec) * 1000
      .select(col("ip"), (col("aggregation.lastEvent").cast(LongType) + banTimeSec) * 1000, col("incident"))

  val output =
    filtered
      .writeStream
      .outputMode("update")
      .foreach(new CassandraSinkForeach(appConf, "stopbot", "bots", Set("ip", "period", "reason"), banRecordTTL))
      .start()

  output.awaitTermination()
}
