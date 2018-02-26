package com.griddynamics.stopbot.spark.logic

import java.time.Instant

import cats.kernel.Semigroup
import com.griddynamics.stopbot.model.{Event, EventAggregation, EventType, Incident}
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
        input.transform { (rdd, window) =>
          val windowBarrier = (window.milliseconds / 1000) - window.milliseconds
          rdd.filter(windowBarrier < _.time)
        }
      } else {
        input
      }

    /* convert to a semigroup (ease to aggregate) */
    val batchRDD = skippedOld.map {
      case Event(EventType.Click, ip, time, _) => (ip, EventAggregation(1L, 0L, time, time))
      case Event(EventType.Watch, ip, time, _) => (ip, EventAggregation(0L, 1L, time, time))
      case Event(_, ip, time, _) => (ip, EventAggregation(0L, 0L, time, time))
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
            Some(Incident(ip, to * 1000, s"too much events from ${Instant.ofEpochSecond(from)} to ${Instant.ofEpochSecond(to)}"))
          } else if (rate <= minRate) {
            Some(Incident(ip, to * 1000, s"too suspicious rate from ${Instant.ofEpochSecond(from)} to ${Instant.ofEpochSecond(to)}"))
          } else None
        } else {
          None
        }
    }
  }
}
