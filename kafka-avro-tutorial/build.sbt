import sbt.Resolver

name := "kafka-avro-tutorial"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

val confluentVersion = "5.4.0"
val kafkaVersion = "2.4.0"
val avroVersion = "1.9.2"

libraryDependencies ++= Seq("org.apache.avro" % "avro" % avroVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30"
)


AvroConfig / sourceDirectory := file("src/main/resources/avro")