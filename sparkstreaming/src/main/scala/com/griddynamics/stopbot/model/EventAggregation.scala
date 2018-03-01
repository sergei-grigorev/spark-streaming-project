package com.griddynamics.stopbot.model

import java.sql.Timestamp

import cats.kernel.Semigroup
import com.griddynamics.stopbot.implicits._

/**
  * Event aggregation model (semigroup).
  *
  * @param clicks  click events
  * @param watches watch events
  */
case class EventAggregation(clicks: Long, watches: Long, firstEvent: Timestamp, lastEvent: Timestamp)

object EventAggregation {
  implicit val group: Semigroup[EventAggregation] = new Semigroup[EventAggregation] {
    override def combine(x: EventAggregation, y: EventAggregation): EventAggregation = {
      EventAggregation(
        x.clicks + y.clicks,
        x.watches + y.watches,
        x.firstEvent min y.firstEvent,
        x.lastEvent max y.lastEvent)
    }
  }
}
