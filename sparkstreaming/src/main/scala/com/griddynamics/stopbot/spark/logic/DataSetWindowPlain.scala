package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{window => byWindow, _}

import scala.concurrent.duration.Duration

object DataSetWindowPlain {

  case class Message(ip: String, value: Message.Event)

  object Message {

    case class Event(`type`: String, unix_time: Long)

  }

  case class ToAggregate(ip: String, clicks: Long, watches: Long, eventTime: Timestamp)

  case class Aggregated(ip: String, clicks: Long, watches: Long, firstEvent: Timestamp, lastEvent: Timestamp)

  case class Incident(ip: String, period: Long, reason: String)

  def findIncidents(input: Dataset[Message],
                    window: Duration,
                    slide: Duration,
                    watermark: Duration,
                    minEvents: Long,
                    maxEvents: Long,
                    minRate: Double): Dataset[Incident] = {

    import input.sparkSession.implicits._

    val aggregated =
      input
        .filter(m => m.ip != null && m.value != null)
        .map { m =>
          ToAggregate(
            m.ip,
            if (m.value.`type` == "click") 1 else 0,
            if (m.value.`type` == "watch") 1 else 0,
            Timestamp.from(Instant.ofEpochSecond(m.value.unix_time))
          )
        }
        .withWatermark("eventTime", watermark.toString)
        .groupBy(
          col("ip"),
          byWindow(col("eventTime"), window.toString, slide.toString))
        .agg(
          sum("clicks").as("clicks"),
          sum("watches").as("watches"),
          min("eventTime").as("firstEvent"),
          max("eventTime").as("lastEvent")
        ).as[Aggregated]

    aggregated
      .flatMap { a =>
        val eventsCount = a.clicks + a.watches
        if (eventsCount > minEvents) {
          val rate = if (a.clicks > 0) (a.watches : Double) / a.clicks else a.watches

          /* cassandra timestamp uses milliseconds */
          if (eventsCount > maxEvents) {
            Some(Incident(a.ip, a.lastEvent.getTime, s"too much events from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else if (rate < minRate) {
            Some(Incident(a.ip, a.lastEvent.getTime, s"too suspicious rate from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else None
        } else {
          None
        }
      }
  }
}
