package com.griddynamics.stopbot.spark.stream

import com.griddynamics.stopbot.implicits._
import com.griddynamics.stopbot.model.MessageStructType
import com.griddynamics.stopbot.spark.logic.StructureWindowSql
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{ LongType, StringType }

import scala.collection.JavaConverters._

/**
 * Structure streaming variant of the same task.
 */
object KafkaSqlToCassandra extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val debug = appConf.getBoolean("debug-mode")
  val banTimeSec = appConf.getDuration("app.ban-time").getSeconds
  val banRecordTTL = banTimeSec

  val sparkBuilder = SparkSession
    .builder
    .config("spark.sql.shuffle.partitions", 3)
    .appName(appConf.getString("app.name"))

  val spark =
    if (debug) {
      sparkBuilder
        .master("local[2]")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
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
    .option("maxOffsetsPerTrigger", appConf.getLong("kafka.max-offset"))
    .load()

  /* key = user_ip, value =  */
  val parsed =
    df
      .select(
        col("key").cast(StringType),
        from_json(col("value").cast(StringType), schema = MessageStructType.schema).alias("value"))
      .selectExpr("key as ip", "value.type as action", "cast(value.unix_time as timestamp) as eventTime")

  val filtered = StructureWindowSql
    .findIncidents(
      input = parsed,
      window = appConf.getDuration("spark.window"),
      slide = appConf.getDuration("spark.slide"),
      watermark = appConf.getDuration("spark.watermark"),
      minEvents = appConf.getLong("app.min-events"),
      maxEvents = appConf.getLong("app.max-events"),
      minRate = appConf.getDouble("app.min-rate"))

  val output =
    filtered
      .select(col("ip"), (col("lastEvent").cast(LongType) + banTimeSec) * 1000, col("reason"))
      .writeStream
      .outputMode(OutputMode.Update())
      .format("com.griddynamics.stopbot.sink.CassandraSinkProvider")
      .option("cassandra.server", appConf.getString("cassandra.server"))
      .option("cassandra.keyspace", "stopbot")
      .option("cassandra.table", "bots")
      .option("cassandra.columns", "ip, period, reason")
      .option("cassandra.ttl", banRecordTTL)
      .start()

  output.awaitTermination()
}
