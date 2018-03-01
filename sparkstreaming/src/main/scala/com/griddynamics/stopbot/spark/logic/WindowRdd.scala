package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.Instant

import cats.kernel.Semigroup
import com.griddynamics.stopbot.model._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

/**
  * RDD way to count events.
  */
object WindowRdd {
  def findIncidents(input: DStream[Event],
                    window: Duration,
                    slide: Duration,
                    minEvents: Long,
                    maxEvents: Long,
                    minRate: Double,
                    skipOld: Boolean = false): DStream[Incident] = {

    /* skip records having time before a window */
    val skippedOld =
      if (skipOld) {
        input.transform { (rdd, windowMs) =>
          val windowBarrier = Timestamp.from(Instant.ofEpochMilli(windowMs.milliseconds - window.milliseconds))
          rdd.filter(!_.eventTime.before(windowBarrier))
        }
      } else {
        input
      }

    /* convert to a semigroup (ease to aggregate) */
    val batchRDD = skippedOld.map {
      case Event(ip, EventType.Click, time) => (ip, EventAggregation(1L, 0L, time, time))
      case Event(ip, EventType.Watch, time) => (ip, EventAggregation(0L, 1L, time, time))
      case Event(ip, _, time) => (ip, EventAggregation(0L, 0L, time, time))
    }

    /* aggregate (we don't need shuffling cause we already combined ip by partitions */
    val windowRdd =
      batchRDD.reduceByKeyAndWindow(
        Semigroup[EventAggregation].combine(_, _),
        window,
        slide
      )

    /* find incidents */
    windowRdd.flatMap {
      case (ip, EventAggregation(clicks, watches, from, to)) =>
        val eventsCount = clicks + watches
        if (eventsCount > minEvents) {
          val rate = if (clicks > 0) (watches: Double) / clicks else watches

          /* cassandra timestamp uses milliseconds */
          if (eventsCount >= maxEvents) {
            Some(
              Incident(
                ip,
                to,
                s"too much events: $eventsCount from $from to $to"))
          } else if (rate <= minRate) {
            Some(
              Incident(
                ip,
                to,
                s"too suspicious rate: $rate from $from to $to"))
          } else None
        } else {
          None
        }
    }
  }
}
