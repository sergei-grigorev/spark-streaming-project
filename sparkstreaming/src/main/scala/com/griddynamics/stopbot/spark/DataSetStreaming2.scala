package com.griddynamics.stopbot.spark

import java.sql.Timestamp
import java.time.Instant

import com.griddynamics.stopbot.model.EventStructType
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object DataSetStreaming2 extends App {
  val logger = Logger("streaming")

  case class Event(`type`: String, unix_time: Long)

  case class Message(ip: String, value: Event)

  case class ToAggregate(ip: String, clicks: Long, watches: Long, eventTime: Timestamp)

  case class Aggregated(ip: String, clicks: Long, watches: Long, firstEvent: Timestamp, lastEvent: Timestamp)

  case class Incident(ip: String, period: Long, reason: String)

  /* application configuration */
  val appConf = ConfigFactory.load
  val windowSec = appConf.getDuration("spark.window").getSeconds
  val slideSec = appConf.getDuration("spark.slide").getSeconds
  val watermark = appConf.getDuration("spark.watermark").getSeconds
  val maxEvents = appConf.getLong("app.max-events")
  val minEvents = appConf.getLong("app.min-events")
  val minRate = appConf.getLong("app.min-rate")
  val banTimeMs = appConf.getDuration("app.ban-time").toMillis

  val spark = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .master("local[*]")
    .appName("SparkStructureStreaming")
    .getOrCreate()

  import spark.implicits._

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
    ).as[Message]

  val aggregated =
    parsed
      .filter(m => m.ip != null && m.value != null)
      .map { m =>
        ToAggregate(
          m.ip,
          if (m.value.`type` == "click") 1 else 0,
          if (m.value.`type` == "watch") 1 else 0,
          Timestamp.from(Instant.ofEpochSecond(m.value.unix_time))
        )
      }
      .withWatermark("eventTime", s"$watermark seconds")
      .groupBy(
        col("ip"),
        window(col("eventTime"), s"$windowSec seconds", s"$slideSec seconds"))
      .agg(
        sum("clicks").as("clicks"),
        sum("watches").as("watches"),
        min("eventTime").as("firstEvent"),
        max("eventTime").as("lastEvent")
      ).as[Aggregated]

  val filtered =
    aggregated
      .flatMap { a =>
        val eventsCount = a.clicks + a.watches
        if (eventsCount > minEvents) {
          val rate = if (a.clicks > 0) a.watches / a.clicks else a.watches

          /* cassandra timestamp uses milliseconds */
          if (eventsCount > maxEvents) {
            Some(Incident(a.ip, a.lastEvent.getTime + banTimeMs, s"too much events from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else if (rate < minRate) {
            Some(Incident(a.ip, a.lastEvent.getTime + banTimeMs, s"too suspicious rate from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else None
        } else {
          None
        }
      }

  val output =
    filtered
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .option("numRows", 50)
      .start()

  /* identify execution plan */
  filtered.explain(true)

  output.awaitTermination()
}
