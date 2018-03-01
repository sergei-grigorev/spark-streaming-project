package com.griddynamics.stopbot.source

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{ DataSourceRegister, StreamSourceProvider }
import org.apache.spark.sql.types._

class MySourceProvider extends DataSourceRegister with StreamSourceProvider {

  override def shortName(): String = "my-provider"

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): (String, StructType) =
    (shortName(), MySource.mySchema)

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source =
    new MySource(sqlContext)
}