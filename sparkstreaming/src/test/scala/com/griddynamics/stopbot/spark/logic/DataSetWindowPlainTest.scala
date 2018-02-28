package com.griddynamics.stopbot.spark.logic

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

import scala.concurrent.duration._

class DataSetWindowPlainTest extends FunSuite with DatasetSuiteBase {

  import spark.implicits._

  test("too much events") {
    val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    /* input data for group by processing time */
    val input =
      spark.createDataset(
        (0 to 4)
          .map(n => DataSetWindowPlain.Message(
            "10.10.10.10",
            DataSetWindowPlain.Message.Event("watch", start.plusSeconds(n).getEpochSecond))
          )
      )

    /* result */
    val actual =
      DataSetWindowPlainTest.callWindowRdd(input)
        .distinct()
        .sort(col("lastEvent"), col("reason"))

    /* our expectation */
    val expected =
      spark.createDataset(
        Seq(
          DataSetWindowPlain.Incident(
            "10.10.10.10",
            start.plusSeconds(3).getEpochSecond,
            s"too much events: 4 from $start to ${start.plusSeconds(3)}"),
          DataSetWindowPlain.Incident(
            "10.10.10.10",
            start.plusSeconds(4).getEpochSecond,
            s"too much events: 4 from ${start.plusSeconds(1)} to ${start.plusSeconds(4)}"),
          DataSetWindowPlain.Incident(
            "10.10.10.10",
            start.plusSeconds(4).getEpochSecond,
            s"too much events: 5 from $start to ${start.plusSeconds(4)}"))
      )

    /* compare this two streams */
    assertDatasetEquals(actual, expected)
  }

    test("too small rate") {
      val start = Instant.now().truncatedTo(ChronoUnit.SECONDS)

      /* input data for group by processing time */
      val input =
        spark.createDataset(
          Seq(
            DataSetWindowPlain.Message(
              "10.10.10.10",
              DataSetWindowPlain.Message.Event("watch", start.getEpochSecond)),
            DataSetWindowPlain.Message(
              "10.10.10.10",
              DataSetWindowPlain.Message.Event("click", start.plusSeconds(1).getEpochSecond)),
            DataSetWindowPlain.Message(
              "10.10.10.10",
              DataSetWindowPlain.Message.Event("click", start.plusSeconds(2).getEpochSecond)))
        )

      /* result */
      val actual =
        DataSetWindowPlainTest.callWindowRdd(input)
          .distinct()

      /* our expectation */
      val expected =
        spark.createDataset(
          Seq(
            DataSetWindowPlain.Incident(
              "10.10.10.10",
              start.plusSeconds(2).getEpochSecond,
              s"too small rate: 0.5 from $start to ${start.plusSeconds(2)}")
          )
        )

      /* compare this two streams */
      assertDatasetEquals(actual, expected)
    }
}


object DataSetWindowPlainTest {
  val minEvents = 2
  val maxEvents = 4
  val minRate = 0.5

  def callWindowRdd(input: Dataset[DataSetWindowPlain.Message]): Dataset[DataSetWindowPlain.Incident] =
    DataSetWindowPlain.findIncidents(input, 10.seconds, 1.second, 20.seconds, minEvents, maxEvents, minRate)
}


