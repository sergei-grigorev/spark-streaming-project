package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.griddynamics.stopbot.model.{ Event, EventType, Incident, Message }
import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

/* unit test for RDD case */
class WindowRddTest extends FunSuite with StreamingSuiteBase {

  test("too much events") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    val batch1 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start)))
    val batch2 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start.plusSeconds(1))))
    val batch3 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start.plusSeconds(2))))
    val batch4 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start.plusSeconds(3))))
    val batch5 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start.plusSeconds(4))))

    val input = List(batch1, batch2, batch3, batch4, batch5)

    val expected =
      List(
        Nil,
        Nil,
        Nil,
        List(
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(3)),
            s"too much events: 4 from ${Timestamp.from(start)} to ${Timestamp.from(start.plusSeconds(3))}")),
        List(
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(4)),
            s"too much events: 5 from ${Timestamp.from(start)} to ${Timestamp.from(start.plusSeconds(4))}")))

    testOperation(input, WindowRddTest.callWindowRdd, expected)
  }

  test("too small rate") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    val batch1 = List(Event("10.10.10.10", EventType.Watch, Timestamp.from(start)))
    val batch2 = List(Event("10.10.10.10", EventType.Click, Timestamp.from(start.plusSeconds(1))))
    val batch3 = List(Event("10.10.10.10", EventType.Click, Timestamp.from(start.plusSeconds(2))))

    val input = List(batch1, batch2, batch3)

    val expected =
      List(
        Nil,
        Nil,
        List(
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(2)),
            s"too suspicious rate: 0.5 from ${Timestamp.from(start)} to ${Timestamp.from(start.plusSeconds(2))}")))

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
