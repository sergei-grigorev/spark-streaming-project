package com.griddynamics.stopbot.model

import cats.kernel.Semigroup

/**
 * Event aggregation model (semigroup).
 *
 * @param clicks click events
 * @param watches watch events
 */
case class EventAggregation(clicks: Long, watches: Long, firstEvent: Long, lastEvent: Long)

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
