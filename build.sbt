
ThisBuild / scalaVersion := "2.12.7"

ThisBuild / organization := "com.oldtan"

val flinkVersion = "1.12.2"

val log4jVersion = "2.14.1"

val flink = "org.apache.flink" %% "flink-scala" % flinkVersion

val flinkStreaming = "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Compile

val flinkClient = "org.apache.flink" %% "flink-clients" % flinkVersion

val mysql = "mysql" % "mysql-connector-java" % "8.0.14" % Runtime

val log4j = "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Runtime

val log4jApi = "org.apache.logging.log4j" % "log4j-api" % log4jVersion % Runtime

val log4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Runtime

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test

lazy val toOne = (project in file("toone"))
  .settings(
    name := "NeuStar-toOne",
    version := "1.0",
    libraryDependencies ++= Seq(flink, flinkStreaming, flinkClient, mysql, log4j, log4jApi,log4jImpl, scalaTest)
  )
