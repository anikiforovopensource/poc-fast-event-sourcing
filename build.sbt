name := """overcharched"""

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven"

libraryDependencies ++= Seq(
  "com.pagerduty" %% "eris-pd" % "3.0.0",
  "ch.qos.logback" % "logback-classic" % "1.0.9",
  "com.typesafe.akka" %% "akka-actor" % "2.3.15",
  "com.typesafe.akka" %% "akka-contrib" % "2.3.15",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.15",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.15",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test")
