package com.griddynamics.stopbot.spark.stream

import com.griddynamics.stopbot.implicits._
import com.griddynamics.stopbot.model.EventStructType
import com.griddynamics.stopbot.sink.CassandraSinkForeach
import com.griddynamics.stopbot.spark.logic.StructureWindowUdf
import com.griddynamics.stopbot.spark.stream.KafkaDataSetToConsole.appConf
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object KafkaStructureToCassandra extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val debug = appConf.getBoolean("debug-mode")
  val banTimeSec = appConf.getDuration("app.ban-time").getSeconds
  val banRecordTTL = (banTimeSec / 1000).toInt

  val sparkBuilder = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .appName(appConf.getString("app.name"))

  val spark =
    if (debug) {
      sparkBuilder
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

  /* key = user_ip, value =  */
  val parsed =
    df
      .select(
        col("key").cast(StringType),
        from_json(col("value").cast(StringType), schema = EventStructType.schema).alias("value")
      )
      .withColumn("eventTime", col("value.unix_time").cast(TimestampType))
      .selectExpr("key as ip", "value.type as action", "eventTime")

  /* run structure streaming with custom UDF */
  val filtered = StructureWindowUdf
    .findIncidents(
      input = parsed,
      window = appConf.getDuration("spark.window"),
      slide = appConf.getDuration("spark.slide"),
      watermark = appConf.getDuration("spark.watermark"),
      minEvents = appConf.getLong("app.min-events"),
      maxEvents = appConf.getLong("app.max-events"),
      minRate = appConf.getDouble("app.min-rate")
    )

  val output =
    filtered
      .select(col("ip"), (col("aggregation.lastEvent").cast(LongType) + banTimeSec) * 1000, col("incident"))
      .writeStream
      .outputMode("update")
      .foreach(new CassandraSinkForeach(appConf, "stopbot", "bots", Set("ip", "period", "reason"), banRecordTTL))
      .start()

  output.awaitTermination()
}
