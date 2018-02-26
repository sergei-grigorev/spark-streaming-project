package com.griddynamics.stopbot.spark.logic

import java.time.Instant
import java.time.temporal.{ChronoField, ChronoUnit, TemporalUnit}

import com.griddynamics.stopbot.model.{Event, EventType, Incident}
import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

/* unit test for RDD case */
class WindowRddTest extends FunSuite with StreamingSuiteBase {

  test("too much events") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    val batch1 = List(Event(EventType.Watch, "10.10.10.10", start.getEpochSecond, ""))
    val batch2 = List(Event(EventType.Watch, "10.10.10.10", start.plusSeconds(1).getEpochSecond, ""))
    val batch3 = List(Event(EventType.Watch, "10.10.10.10", start.plusSeconds(2).getEpochSecond, ""))
    val batch4 = List(Event(EventType.Watch, "10.10.10.10", start.plusSeconds(3).getEpochSecond, ""))
    val batch5 = List(Event(EventType.Watch, "10.10.10.10", start.plusSeconds(4).getEpochSecond, ""))

    val input = List(batch1, batch2, batch3, batch4, batch5)

    val expected =
      List(
        Nil,
        Nil,
        Nil,
        List(
          Incident(
            "10.10.10.10",
            start.plusSeconds(3).toEpochMilli,
            s"too much events from ${start.toString} to ${start.plusSeconds(3).toString}")),
        List(
          Incident(
            "10.10.10.10",
            start.plusSeconds(4).toEpochMilli,
            s"too much events from ${start.toString} to ${start.plusSeconds(4).toString}"))
      )

    testOperation(input, WindowRddTest.callWindowRdd, expected)
  }

  test("too small rate") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    val batch1 = List(Event(EventType.Watch, "10.10.10.10", start.getEpochSecond, ""))
    val batch2 = List(Event(EventType.Click, "10.10.10.10", start.plusSeconds(1).getEpochSecond, ""))
    val batch3 = List(Event(EventType.Click, "10.10.10.10", start.plusSeconds(2).getEpochSecond, ""))

    val input = List(batch1, batch2, batch3)

    val expected =
      List(
        Nil,
        Nil,
        List(
          Incident(
            "10.10.10.10",
            start.plusSeconds(2).toEpochMilli,
            s"too suspicious rate from ${start.toString} to ${start.plusSeconds(2).toString}"))
      )

    testOperation(input, WindowRddTest.callWindowRdd, expected)
  }
}

object WindowRddTest {
  val minEvents = 2
  val maxEvents = 4
  val minRate = 0.5

  def callWindowRdd(input: DStream[Event]): DStream[Incident] =
    WindowRdd.findIncidents(input, Seconds(10), Seconds(1), minEvents, maxEvents, minRate)
}
