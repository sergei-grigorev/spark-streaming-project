package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp

import com.griddynamics.stopbot.model.{Event2, Incident}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{window => byWindow, _}

import scala.concurrent.duration.Duration

object DataSetWindowPlain {

  private case class ToAggregate(ip: String, clicks: Long, watches: Long, eventTime: Timestamp)

  private case class Aggregated(ip: String, clicks: Long, watches: Long, firstEvent: Timestamp, lastEvent: Timestamp)

  def findIncidents(input: Dataset[Event2],
                    window: Duration,
                    slide: Duration,
                    watermark: Duration,
                    minEvents: Long,
                    maxEvents: Long,
                    minRate: Double): Dataset[Incident] = {

    import input.sparkSession.implicits._

    val aggregated =
      input
        .filter(m => m.ip != null && m.eventTime != null)
        .map { m =>
          ToAggregate(
            m.ip,
            if (m.action == "click") 1 else 0,
            if (m.action == "watch") 1 else 0,
            m.eventTime
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
          val rate = if (a.clicks > 0) (a.watches: Double) / a.clicks else a.watches

          /* cassandra timestamp uses milliseconds */
          if (eventsCount >= maxEvents) {
            Some(Incident(a.ip, a.lastEvent, s"too much events: $eventsCount from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else if (rate <= minRate) {
            Some(Incident(a.ip, a.lastEvent, s"too small rate: $rate from ${a.firstEvent.toInstant} to ${a.lastEvent.toInstant}"))
          } else None
        } else {
          None
        }
      }
  }
}
