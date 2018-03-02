package com.griddynamics.stopbot.spark.logic

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ Instant, ZoneId }

import com.griddynamics.stopbot.model.{ Event2, Incident }
import com.holdenkarau.spark.testing.{ DataFrameSuiteBase, DatasetSuiteBase }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Dataset }
import org.scalatest.FunSuite

import scala.concurrent.duration._

class StructureStreamingTest extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

  import StructureStreamingTest._
  import spark.implicits._

  /* dataset tests */
  val dataSetImplementations = Seq(
    ("DataSetWindowPlain", dataSetWindowPlain _),
    ("DataSetWindowUdf", dataSetWindowUdf _))

  /* dataframe tests */
  val dataFrameImplementations = Seq(
    ("StructureWindowPlain", structureWindowPlain _),
    ("StructureWindowUdf", structureWindowUdf _),
    ("StructureWindowSql", structureWindowSql _))

  /* list of tests */
  val tests = Seq(StructureStreamingTest.TooMuchEvents, StructureStreamingTest.TooSuspiciousRate)

  /* run tests */
  for (t <- tests) {
    val testName = t.name
    for ((name, runLogic) <- dataSetImplementations) {
      test(s"$name.$testName") {
        val input = spark.createDataset(t.input)
        val expected = spark.createDataset(t.expected)

        val actual =
          runLogic(input)
            .distinct()
            .sort(col("lastEvent"), col("reason"))

        assertDatasetEquals(actual, expected)
      }
    }

    for ((name, runLogic) <- dataFrameImplementations) {
      test(s"$name.$testName") {
        val input = spark.createDataFrame(t.input)
        val expected = spark.createDataFrame(t.expected)

        val actual =
          runLogic(input)
            .distinct()
            .sort(col("lastEvent"), col("reason"))

        assertDataFrameEquals(actual, expected)
      }
    }
  }
}

object StructureStreamingTest {
  val start: Instant = Instant.now().truncatedTo(ChronoUnit.SECONDS)
  val minEvents = 2
  val maxEvents = 4
  val minRate = 0.5
  val window: FiniteDuration = 10.seconds
  val slide: FiniteDuration = 1.second
  val watermark: FiniteDuration = 20.seconds

  val formatDate: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.systemDefault())

  def structureWindowPlain(input: DataFrame): DataFrame =
    StructureWindowPlain.findIncidents(input, window, slide, watermark, minEvents, maxEvents, minRate)

  def structureWindowUdf(input: DataFrame): DataFrame =
    StructureWindowUdf.findIncidents(input, window, slide, watermark, minEvents, maxEvents, minRate)

  def structureWindowSql(input: DataFrame): DataFrame =
    StructureWindowSql.findIncidents(input, window, slide, watermark, minEvents, maxEvents, minRate)

  def dataSetWindowPlain(input: Dataset[Event2]): Dataset[Incident] =
    DataSetWindowPlain.findIncidents(input, window, slide, watermark, minEvents, maxEvents, minRate)

  def dataSetWindowUdf(input: Dataset[Event2]): Dataset[Incident] =
    DataSetWindowUdf.findIncidents(input, window, slide, watermark, minEvents, maxEvents, minRate)

  /**
   * Tests.
   */
  trait TestData {
    val name: String
    val input: Seq[Event2]
    val expected: Seq[Incident]
  }

  /**
   * Test 1: too much events.
   */
  object TooMuchEvents extends TestData {

    override val name: String = "too_much_events"

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
  object TooSuspiciousRate extends TestData {

    override val name: String = "too_suspicious_rate"

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
