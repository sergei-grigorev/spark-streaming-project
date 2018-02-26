package com.griddynamics.stopbot.spark.stream

import com.griddynamics.stopbot.implicits._
import com.griddynamics.stopbot.model.EventStructType
import com.griddynamics.stopbot.spark.logic.DataSetWindowPlain
import com.griddynamics.stopbot.spark.logic.DataSetWindowPlain.Message
import com.griddynamics.stopbot.spark.stream.KafkaRddToCassandra.appConf
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

/**
  * Structure streaming variant of the same task.
  */
object KafkaDataSetToConsole extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val debug = appConf.getBoolean("debug-mode")
  val banTimeMs = appConf.getDuration("app.ban-time").toMillis

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

  val filtered = DataSetWindowPlain
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
      .map(i => i.copy(period = i.period + banTimeMs))
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
