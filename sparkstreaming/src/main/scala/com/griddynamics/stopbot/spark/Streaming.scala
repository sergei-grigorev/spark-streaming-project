package com.griddynamics.stopbot.spark

import java.time.Instant

import cats.kernel.Semigroup
import com.griddynamics.stopbot.model.{Event, EventAggregation, EventType, Incident}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import io.circe.parser._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}

import scala.collection.JavaConverters._

object Streaming extends App {
  val logger = Logger("streaming")

  /* application configuration */
  val appConf = ConfigFactory.load
  val watermarkSec = appConf.getDuration("spark.watermark").getSeconds
  val windowSec = appConf.getDuration("spark.window").getSeconds
  val maxEvents = appConf.getLong("app.max-events")
  val minRate = appConf.getLong("app.min-rate")
  val banTimeMs = appConf.getDuration("app.ban-time").toMillis
  val banRecordTTL = (banTimeMs / 1000).toInt

  /* spark configuration */
  /* TODO: remove master configuration */
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(appConf.getString("app.name"))
    .set("spark.cassandra.connection.host", "127.0.0.1")

  /* spark context */
  val ssc = new StreamingContext(sparkConf, Seconds(appConf.getDuration("spark.batch").getSeconds))

  /* kafka parameters */
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> appConf.getString("kafka.brokers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
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
    decode[Event](r.value()) match {
      case scala.util.Right(value) => value
      case scala.util.Left(reason) =>
        logger.warn("incorrect message format", reason)
        Event(EventType.Unknown, "", 0L, "")
    }
  }.filter(_.eventType != EventType.Unknown)

  /* watermark for an event time and skip records having time before a window */
  val watermarkRDD = parsedRDD.transform { rdd =>
    val time = System.currentTimeMillis()
    val watermarkBarrier = time - (watermarkSec * 1000)
    val windowBarrier = time - (windowSec * 1000)

    /* we have a problem that we don't have a watermark support out of the box */
    rdd
      .filter(watermarkBarrier < _.time)
      .filter(windowBarrier < _.time)
  }

  /* convert to a semigroup (ease to aggregate) */
  val batchRDD = watermarkRDD.map {
    case Event(EventType.Click, ip, time, _) => (ip, EventAggregation(1L, 0L, time, time))
    case Event(EventType.Watch, ip, time, _) => (ip, EventAggregation(0L, 1L, time, time))
    case Event(_, ip, time, _) => (ip, EventAggregation(0L, 0L, time, time))
  }

  /* aggregate */
  val windowRdd =
    batchRDD.reduceByKeyAndWindow(
      Semigroup[EventAggregation].combine(_, _),
      Seconds(windowSec),
      Seconds(appConf.getDuration("spark.slide").getSeconds)
    )

  /* filter suspected */
  val suspectedRdd =
    windowRdd.flatMap {
      case (ip, EventAggregation(clicks, watches, from, to)) =>
        val eventsCount = clicks + watches
        val rate = if (clicks > 0) watches / clicks else 0

        if (eventsCount > maxEvents) {
          Some(Incident(ip, to + banTimeMs, s"too much events from ${Instant.ofEpochMilli(from)} to ${Instant.ofEpochMilli(to)}"))
        } else if (rate < minRate) {
          Some(Incident(ip, to + banTimeMs, s"too suspicious rate from ${Instant.ofEpochMilli(from)} to ${Instant.ofEpochMilli(to)}"))
        } else None
    }

  /* todo check unsuspected by their behaviour */

  /* save to cassandra */
  suspectedRdd.foreachRDD(
    _.saveToCassandra(
      "stopbot",
      "bots",
      SomeColumns("ip", "period", "reason"),
      writeConf = WriteConf(ttl = TTLOption.constant(banRecordTTL))))

  ssc.start()
  ssc.awaitTermination()
}
