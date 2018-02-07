import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.griddynamics.stopbot",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SparkStreaming",
    libraryDependencies ++= Seq(
      sparkCore/* % Provided*/,
      sparkStreaming/* % Provided*/,
      sparkSql/* % Provided*/,
      kafkaStreaming,
      cassandra,
      typeSafeConfig,
      cats,
      logging,
      scalaTest % Test,
      scalaCheck % Test,
      catsLaws % Test
    ) ++ circe
  )
