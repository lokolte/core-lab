name := "kafka-tutorial"

version := "0.1"

scalaVersion := "2.13.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.4.0",
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30"
)
