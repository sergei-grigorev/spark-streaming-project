import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.13.4"

  lazy val sparkVersion = "2.2.1"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  lazy val kafkaStreaming = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
  lazy val kafkaSql = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val cassandra =  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7"

  lazy val cats = "org.typelevel" %% "cats-core" % "1.0.1"
  lazy val catsLaws = "org.typelevel" %% "cats-laws" % "1.0.1"

  lazy val circe = Seq(
    "io.circe" %% "circe-core" % "0.9.1",
    "io.circe" %% "circe-generic" % "0.9.1",
    "io.circe" %% "circe-parser" % "0.9.1"
  )

  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.3.2"

  lazy val logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
}
