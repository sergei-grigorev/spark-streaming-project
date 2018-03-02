package com.griddynamics.stopbot.spark.logic

import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.griddynamics.stopbot.model.{ Event2, Event2WithWindow, Incident }
import com.griddynamics.stopbot.spark.udf.TypeSafeEventAggregationUdf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ window => byWindow, _ }

import scala.concurrent.duration.Duration

object DataSetWindowUdf {
  private val formatDate: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())

  def findIncidents(
    input: Dataset[Event2],
    window: Duration,
    slide: Duration,
    watermark: Duration,
    minEvents: Long,
    maxEvents: Long,
    minRate: Double): Dataset[Incident] = {

    import input.sparkSession.implicits._

    val aggregated =
      input
        .filter(m => m.ip != null && m.action != null)
        .withWatermark("eventTime", watermark.toString)
        .withColumn("window", byWindow(col("eventTime"), window.toString, slide.toString))
        .as[Event2WithWindow]
        .groupByKey(v => (v.ip, v.window))
        .agg(
          TypeSafeEventAggregationUdf.toColumn)

    aggregated
      .flatMap {
        case ((ip, _), a) =>
          val eventsCount = a.clicks + a.watches
          if (eventsCount > minEvents) {
            val rate = if (a.clicks > 0) (a.watches: Double) / a.clicks else a.watches

            /* cassandra timestamp uses milliseconds */
            if (eventsCount >= maxEvents) {
              Some(
                Incident(
                  ip,
                  a.lastEvent,
                  s"too much events: $eventsCount " +
                    s"from ${formatDate.format(a.firstEvent.toInstant)} " +
                    s"to ${formatDate.format(a.lastEvent.toInstant)}"))
            } else if (rate <= minRate) {
              Some(
                Incident(
                  ip,
                  a.lastEvent,
                  s"too suspicious rate: $rate " +
                    s"from ${formatDate.format(a.firstEvent.toInstant)} " +
                    s"to ${formatDate.format(a.lastEvent.toInstant)}"))
            } else None
          } else {
            None
          }
      }
  }
}
