package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import StructureWindowUdfTest.formatDate

import com.griddynamics.stopbot.model.{Event2, Incident}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

import scala.concurrent.duration._

class StructureWindowPlainTest extends FunSuite with DataFrameSuiteBase {
  test("too much events") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    /* input data for group by processing time */
    val input =
      spark.createDataFrame((0 to 4).map(n => Event2("10.10.10.10", "watch", Timestamp.from(start.plusSeconds(n)))))

    /* result */
    val actual =
      StructureWindowPlainTest.callWindowRdd(input)
        .distinct()
        .sort(col("lastEvent"), col("reason"))

    /* our expectation */
    val expected =
      spark.createDataFrame(
        Seq(
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(3)),
            s"too much events: 4 from ${formatDate.format(start)} to ${formatDate.format(start.plusSeconds(3))}"),
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(4)),
            s"too much events: 4 from ${formatDate.format(start.plusSeconds(1))} to ${formatDate.format(start.plusSeconds(4))}"),
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(4)),
            s"too much events: 5 from ${formatDate.format(start)} to ${formatDate.format(start.plusSeconds(4))}"))
      )

    /* compare this two streams */
    assertDataFrameEquals(actual, expected)
  }

  test("too small rate") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    /* input data for group by processing time */
    val input =
      spark.createDataFrame(
        Seq(
          Event2("10.10.10.10", "watch", Timestamp.from(start)),
          Event2("10.10.10.10", "click", Timestamp.from(start.plusSeconds(1))),
          Event2("10.10.10.10", "click", Timestamp.from(start.plusSeconds(2)))))

    /* result */
    val actual =
      StructureWindowPlainTest.callWindowRdd(input)
        .distinct()

    /* our expectation */
    val expected =
      spark.createDataFrame(
        Seq(
          Incident(
            "10.10.10.10",
            Timestamp.from(start.plusSeconds(2)),
            s"too small rate: 0.5 from ${formatDate.format(start)} to ${formatDate.format(start.plusSeconds(2))}")
        )
      )

    /* compare this two streams */
    assertDataFrameEquals(actual, expected)
  }
}

object StructureWindowPlainTest {
  val minEvents = 2
  val maxEvents = 4
  val minRate = 0.5

  val formatDate: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())

  def callWindowRdd(input: DataFrame): DataFrame =
    StructureWindowPlain.findIncidents(input, 10.seconds, 1.second, 20.seconds, minEvents, maxEvents, minRate)
}
