package com.griddynamics.stopbot.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.execution.streaming.{ LongOffset, Offset, Source }
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }
import org.apache.spark.sql.functions._

class MySource(sqLContext: SQLContext) extends Source with Logging {
  private var endOffset: Option[Long] = Some(0L + MySource.step)

  override def schema: StructType = MySource.mySchema

  override def getOffset: Option[Offset] =
    endOffset.map(LongOffset.apply)

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fromPosition = start.flatMap(LongOffset.convert(_).map(_.offset)).getOrElse(0L)
    val toPosition = LongOffset.convert(end).map(_.offset).getOrElse(0L)
    logInfo(s"read from $fromPosition to $toPosition")

    endOffset = Some(toPosition + 10)
    sqLContext.range(fromPosition, toPosition)
      .select(
        lit("key").as("key"),
        col("id").as("value"),
        lit("topic").as("topic"),
        col("id").as("offset"))
  }

  override def commit(end: Offset): Unit = {
    super.commit(end)
    logInfo(s"commited offset: $end")
  }

  override def stop(): Unit = {}
}

object MySource {
  private val step = 10L

  def mySchema: StructType = StructType(Seq(
    StructField("key", StringType),
    StructField("value", StringType),
    StructField("topic", StringType),
    StructField("offset", LongType)))
}
