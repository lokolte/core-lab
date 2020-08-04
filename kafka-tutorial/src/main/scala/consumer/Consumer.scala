package consumer

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

object Consumer extends App {

  implicit val executionContext = scala.concurrent.ExecutionContext.global

  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)

  val consumer = new KafkaConsumer[String, String](properties)

  //def startConsumer(consumer: KafkaConsumer[String, String])(implicit ec: ExecutionContext) = Future {

  consumer.subscribe(util.Arrays.asList("first_topic"))

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

    for (data: ConsumerRecord[String, String] <- records.iterator().asScala) {
      val jsonRepresentation = data.value().toString
      //val record             = Json.parse(jsonRepresentation).validate[DsarResponse].get
      print("Processed: Record readed key: %s", data.key().toString)
      print("Processed: Record readed data: %s", jsonRepresentation)
    }
  }

  //}

  //startConsumer(consumer)

}