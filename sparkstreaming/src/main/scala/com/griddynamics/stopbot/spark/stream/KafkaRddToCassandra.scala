package com.griddynamics.stopbot.spark.stream

import java.sql.Timestamp
import java.time.Instant

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{ TTLOption, WriteConf }
import com.griddynamics.stopbot.model.{ CassandraRecord, Event, EventType, Message }
import com.griddynamics.stopbot.spark.logic.WindowRdd
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import io.circe.parser._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import scala.collection.JavaConverters._

object KafkaRddToCassandra extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val debug = appConf.getBoolean("debug-mode")

  val banTimeMs = appConf.getDuration("app.ban-time").toMillis
  val banRecordTTL = (banTimeMs / 1000).toInt

  /* spark configuration */
  val sparkRunMode =
    if (debug) {
      new SparkConf()
        .setMaster("local[2]")
    } else {
      new SparkConf()
    }

  val sparkConf =
    sparkRunMode
      .setAppName(appConf.getString("app.name"))
      .set("spark.cassandra.connection.host", appConf.getString("cassandra.server"))

  /* spark context */
  val ssc = new StreamingContext(sparkConf, Seconds(appConf.getDuration("spark.batch").getSeconds))

  /* kafka parameters */
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> appConf.getString("kafka.brokers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "max.partition.fetch.bytes" -> appConf.getString("kafka.max.partition.fetch.bytes"),
    "group.id" -> appConf.getString("kafka.group.id"),
    "auto.offset.reset" -> appConf.getString("kafka.offset.reset"),
    "enable.auto.commit" -> (false: java.lang.Boolean))

  /* topics */
  val topics = appConf.getStringList("kafka.topic").asScala

  /* kafka stream */
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  /* parse events and skip incorrect messages */
  val parsedRDD = stream.map { r =>
    decode[Message](r.value()) match {
      case scala.util.Right(value) => value
      case scala.util.Left(reason) =>
        logger.warn("incorrect message format", reason)
        Message(EventType.Unknown, "", 0L, "")
    }
  }.filter(_.eventType != EventType.Unknown)
    .map(r => Event(r.ip, r.eventType, Timestamp.from(Instant.ofEpochSecond(r.time))))

  val suspectedRdd =
    WindowRdd.findIncidents(
      input = parsedRDD,
      window = Seconds(appConf.getDuration("spark.window").getSeconds),
      slide = Seconds(appConf.getDuration("spark.slide").getSeconds),
      minEvents = appConf.getLong("app.min-events"),
      maxEvents = appConf.getLong("app.max-events"),
      minRate = appConf.getDouble("app.min-rate"))

  /* save to cassandra */
  suspectedRdd
    .map(i => CassandraRecord(i.ip, i.lastEvent.toInstant.toEpochMilli + banTimeMs, i.reason))
    .foreachRDD(
      _.saveToCassandra(
        "stopbot",
        "bots",
        SomeColumns("ip", "period", "reason"),
        writeConf = WriteConf(ttl = TTLOption.constant(banRecordTTL))))

  ssc.start()
  ssc.awaitTermination()
}
