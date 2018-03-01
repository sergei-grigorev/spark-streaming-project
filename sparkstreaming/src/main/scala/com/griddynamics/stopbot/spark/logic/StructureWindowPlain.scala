package com.griddynamics.stopbot.spark.logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ window => byWindow, _ }

import scala.concurrent.duration.Duration

object StructureWindowPlain {

  /**
   * Input model - [[com.griddynamics.stopbot.model.Event2]].
   * Output model - [[com.griddynamics.stopbot.model.Incident]].
   */
  def findIncidents(
    input: DataFrame,
    window: Duration,
    slide: Duration,
    watermark: Duration,
    minEvents: Long,
    maxEvents: Long,
    minRate: Double): DataFrame = {
    val aggregated =
      input
        .withColumn("clicks", when(col("action") === "click", 1).otherwise(0))
        .withColumn("watches", when(col("action") === "watch", 1).otherwise(0))
        .drop(col("action"))
        .withWatermark("eventTime", watermark.toString)
        .groupBy(
          col("ip"),
          byWindow(col("eventTime"), window.toString, slide.toString))
        .agg(
          sum("clicks").as("clicks"),
          sum("watches").as("watches"),
          min("eventTime").as("firstEvent"),
          max("eventTime").as("lastEvent"))

    aggregated
      .withColumn("total_events", col("clicks") + col("watches"))
      .filter(col("total_events") > minEvents)
      .withColumn(
        "rate",
        when(col("clicks") > 0, col("watches") / col("clicks"))
          .otherwise(col("watches")))
      .withColumn(
        "incident",
        when(
          col("total_events") >= maxEvents,
          concat(
            lit("too much events: "),
            col("total_events"),
            lit(" from "),
            col("firstEvent"),
            lit(" to "),
            col("lastEvent"))).when(
            col("rate") <= minRate,
            concat(
              lit("too suspicious rate: "),
              col("rate"),
              lit(" from "),
              col("firstEvent"),
              lit(" to "),
              col("lastEvent"))).otherwise(null))
      .filter(col("incident").isNotNull)
      .select(col("ip"), col("lastEvent"), col("incident").as("reason"))
  }
}
