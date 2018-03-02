package com.griddynamics.stopbot.spark.udf

import java.sql.Timestamp

import com.griddynamics.stopbot.model.Event2WithWindow
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{ Encoder, Encoders }

case class Aggregated(clicks: Long, watches: Long, firstEvent: Timestamp, lastEvent: Timestamp)

case class Buffer(var clicks: Long, var watches: Long, var firstEvent: Timestamp, var lastEvent: Timestamp)

object TypeSafeEventAggregationUdf extends Aggregator[Event2WithWindow, Buffer, Aggregated] {

  override def zero: Buffer = Buffer(0L, 0L, null, null)

  override def reduce(b: Buffer, a: Event2WithWindow): Buffer = {
    /* aggregate count clicks and watches */
    if (a.action == "click") {
      b.clicks = b.clicks + 1
    } else if (a.action == "watch") {
      b.watches = b.watches + 1
    } else {}

    val actionEvent = a.eventTime
    if (b.firstEvent != null && b.lastEvent != null) {
      /* firstEvent min actionEvent */
      if (actionEvent.before(b.firstEvent)) b.firstEvent = actionEvent

      /* lastEvent max actionEvent */
      if (actionEvent.after(b.lastEvent)) b.lastEvent = actionEvent
    } else {
      b.firstEvent = actionEvent
      b.lastEvent = actionEvent
    }

    b
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    /* aggregate count clicks and watches */
    b1.clicks = b1.clicks + b2.clicks
    b1.watches = b1.watches + b2.watches

    /* firstEvent is min or nothing changes */
    if (b1.firstEvent != null && b2.firstEvent != null) {
      if (b2.firstEvent.before(b1.firstEvent)) {
        b1.firstEvent = b2.firstEvent
      }
    } else if (b2.firstEvent != null) {
      b1.firstEvent = b2.firstEvent
    }

    /* lastEvent is max or nothing changes */
    if (b1.lastEvent != null && b2.lastEvent != null) {
      if (b2.lastEvent.after(b1.lastEvent)) {
        b1.lastEvent = b2.lastEvent
      }
    } else if (b2.lastEvent != null) {
      b1.lastEvent = b2.lastEvent
    }

    b1
  }

  override def finish(reduction: Buffer): Aggregated =
    Aggregated(reduction.clicks, reduction.watches, reduction.firstEvent, reduction.lastEvent)

  override def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]

  override def outputEncoder: Encoder[Aggregated] = Encoders.product[Aggregated]
}
