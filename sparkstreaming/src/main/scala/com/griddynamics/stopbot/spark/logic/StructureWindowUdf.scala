package com.griddynamics.stopbot.spark.logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{window => byWindow, _}

import scala.concurrent.duration.Duration

object StructureWindowUdf {

  def findIncidents(input: DataFrame,
                    window: Duration,
                    slide: Duration,
                    watermark: Duration,
                    minEvents: Long,
                    maxEvents: Long,
                    minRate: Long): DataFrame = {
    val aggregated =
      input
        .withWatermark("eventTime", watermark.toString)
        .groupBy(
          col("ip"),
          byWindow(col("eventTime"), window.toString, slide.toString))
        .agg(EventAggregationUdf(col("action"), col("eventTime")).alias("aggregation"))

    aggregated
      .withColumn("total_events", col("aggregation.clicks") + col("aggregation.watches"))
      .filter(col("total_events") > minEvents)
      .withColumn(
        "rate",
        when(col("aggregation.clicks") > 0, col("aggregation.watches") / col("aggregation.clicks"))
          .otherwise(col("aggregation.watches")))
      .withColumn(
        "incident",
        when(
          col("total_events") > maxEvents,
          concat(
            lit("too much events: "),
            col("total_events"),
            lit(" from "),
            col("aggregation.firstEvent"),
            lit(" to "),
            col("aggregation.lastEvent"))
        ).when(
          col("rate") < minRate,
          concat(
            lit("too small rate: "),
            col("rate"),
            lit(" from "),
            col("aggregation.firstEvent"),
            lit(" to "),
            col("aggregation.lastEvent"))
        ).otherwise(null))
      .filter(col("incident").isNotNull)
  }
}
