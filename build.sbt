version := "0.1"
name := "bd-assignment"
description := "Team dublin bigdata 2nd assignment."

scalaVersion := "2.12.10"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature"
)

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-streaming" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4"
).map(_ % "compile")

libraryDependencies ++= Seq(
  "com.rometools" % "rome" % "1.8.0",
  "org.jsoup" % "jsoup" % "1.10.3",
  "log4j" % "log4j" % "1.2.17"
)

libraryDependencies ++= Seq(
  "org.mockito" % "mockito-core" % "2.8.47",
  "org.mockito" % "mockito-inline" % "2.8.47",
  "org.scalatest" %% "scalatest" % "2.2.1"
).map(_ % "test")
