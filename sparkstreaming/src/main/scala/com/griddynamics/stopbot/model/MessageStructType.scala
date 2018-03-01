package com.griddynamics.stopbot.model

import org.apache.spark.sql.types.{LongType, StringType, StructType}

object MessageStructType {
  val schema: StructType = new StructType()
    .add("type", StringType, nullable = false)
    .add("ip", StringType, nullable = false)
    .add("unix_time", LongType, nullable = false)
    .add("url", StringType, nullable = false)
}
