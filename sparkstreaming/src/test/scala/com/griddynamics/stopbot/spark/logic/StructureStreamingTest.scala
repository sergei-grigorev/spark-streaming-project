package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId}

import com.griddynamics.stopbot.model.{Event2, Incident}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite

import scala.concurrent.duration._

class StructureStreamingTest extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

  import StructureStreamingTest._
  import spark.implicits._

  //
  // DataSetWindowPlain
  //
  test("DataSetWindowPlain: too much events") {
    /* input data for group by processing time */
    val input = spark.createDataset(TooMuchEvents.input)

    /* result */
    val actual =
      dataSetWindowPlain(input)
        .distinct()
        .sort(col("lastEvent"), col("reason"))

    /* our expectation */
    val expected = spark.createDataset(TooMuchEvents.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

  test("DataSetWindowPlain: too suspicious rate") {
    /* input data for group by processing time */
    val input = spark.createDataset(TooSuspiciousRate.input)

    /* result */
    val actual =
      dataSetWindowPlain(input)
        .distinct()

    /* our expectation */
    val expected = spark.createDataset(TooSuspiciousRate.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

  //
  // StructureWindowPlain
  //
  test("StructureWindowPlain: too much events") {
    /* input data for group by processing time */
    val input = spark.createDataFrame(TooMuchEvents.input)

    /* result */
    val actual =
      structureWindowPlain(input)
        .distinct()
        .sort(col("lastEvent"), col("reason"))

    /* our expectation */
    val expected = spark.createDataFrame(TooMuchEvents.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

  test("StructureWindowPlain: too suspicious rate") {
    /* input data for group by processing time */
    val input = spark.createDataFrame(TooSuspiciousRate.input)

    /* result */
    val actual =
      structureWindowPlain(input)
        .distinct()

    /* our expectation */
    val expected = spark.createDataFrame(TooSuspiciousRate.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

  //
  // StructureWindowUdf
  //
  test("StructureWindowUdf: too much events") {
    /* input data for group by processing time */
    val input = spark.createDataFrame(TooMuchEvents.input)

    /* result */
    val actual =
      structureWindowUdf(input)
        .distinct()
        .sort(col("lastEvent"), col("reason"))

    /* our expectation */
    val expected = spark.createDataFrame(TooMuchEvents.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

  test("StructureWindowUdf: too suspicious rate") {
    /* input data for group by processing time */
    val input = spark.createDataFrame(TooSuspiciousRate.input)

    /* result */
    val actual =
      structureWindowUdf(input)
        .distinct()

    /* our expectation */
    val expected = spark.createDataFrame(TooSuspiciousRate.expected)

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }
}

object StructureStreamingTest {
  val start: Instant = Instant.now().truncatedTo(ChronoUnit.SECONDS)
  val minEvents = 2
  val maxEvents = 4
  val minRate = 0.5

  val formatDate: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())

  def structureWindowPlain(input: DataFrame): DataFrame =
    StructureWindowPlain.findIncidents(input, 10.seconds, 1.second, 20.seconds, minEvents, maxEvents, minRate)

  def dataSetWindowPlain(input: Dataset[Event2]): Dataset[Incident] =
    DataSetWindowPlain.findIncidents(input, 10.seconds, 1.second, 20.seconds, minEvents, maxEvents, minRate)

  def structureWindowUdf(input: DataFrame): DataFrame =
    StructureWindowUdf.findIncidents(input, 10.seconds, 1.second, 20.seconds, minEvents, maxEvents, minRate)

  /**
    * Test 1: too much events.
    */
  object TooMuchEvents {
    val input: Seq[Event2] =
      (0 to 4)
        .map(n => Event2(
          "10.10.10.10",
          "watch",
          Timestamp.from(start.plusSeconds(n))))

    val expected: Seq[Incident] =
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
  }

  /**
    * Test 2: too suspicious rate.
    */
  object TooSuspiciousRate {
    val input: Seq[Event2] =
      Seq(
        Event2(
          "10.10.10.10",
          "watch",
          Timestamp.from(start)),
        Event2(
          "10.10.10.10",
          "click",
          Timestamp.from(start.plusSeconds(1))),
        Event2(
          "10.10.10.10",
          "click",
          Timestamp.from(start.plusSeconds(2))))

    val expected: Seq[Incident] = Seq(
      Incident(
        "10.10.10.10",
        Timestamp.from(start.plusSeconds(2)),
        s"too suspicious rate: 0.5 from ${formatDate.format(start)} to ${formatDate.format(start.plusSeconds(2))}"))
  }

}
