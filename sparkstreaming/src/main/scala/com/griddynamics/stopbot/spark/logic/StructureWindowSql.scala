package com.griddynamics.stopbot.spark.logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ window => byWindow, _ }

import scala.concurrent.duration.Duration

object StructureWindowSql {

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
      input.selectExpr(
        "ip",
        "eventTime",
        "case when action = 'click' then 1 else 0 end as clicks",
        "case when action = 'watch' then 1 else 0 end as watches")
        .withWatermark("eventTime", watermark.toString)
        .groupBy(
          col("ip"),
          byWindow(col("eventTime"), window.toString, slide.toString))
        .agg(
          expr("sum(clicks) as clicks"),
          expr("sum(watches) as watches"),
          expr("min(eventTime) as firstEvent"),
          expr("max(eventTime) as lastEvent"))

    aggregated
      .withColumn("total_events", expr("clicks + watches"))
      .filter(col("total_events") > minEvents)
      .withColumn(
        "rate",
        expr("case when clicks > 0 then watches / clicks else watches end as rate"))
      .withColumn(
        "incident",
        expr(
          s"""
             | case
             |  when total_events >= $maxEvents
             |    then concat('too much events: ', total_events, ' from ', firstEvent, ' to ', lastEvent)
             |  when rate <= $minRate
             |    then concat('too suspicious rate: ', rate, ' from ', firstEvent, ' to ', lastEvent)
             |  else null
             |  end
        """.stripMargin))
      .filter(expr("incident is not null"))
      .selectExpr("ip", "lastEvent", "incident as reason")
  }
}
