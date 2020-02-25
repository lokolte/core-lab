package producer

import java.util.Properties

import com.ww.User
import io.confluent.kafka.serializers.KafkaAvroSerializer
import util.KafkaConstants._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object KafkaProducerV2 extends App {
  val logger = LoggerFactory.getLogger(KafkaProducerV2.getClass)

  //Set Kafka properties
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  props.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  //create the producer
  val producer = new KafkaProducer[String, User](props)

  //create the producer record
  val user : User = new User("Test", "LTest", "111-111-1111")

  val record: ProducerRecord[String, User] = new ProducerRecord(TOPIC, user)

  //publish the record
  producer.send(record, new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null)
        logger.info(
          s"""
             |*********
             |Topic: ${metadata.topic()}

             |Partition: ${metadata.partition()}

             |Offset: ${
            metadata.offset()}
             |Timestamp: ${metadata.
            timestamp()
          }
            """.stripMargin)
      else
        logger.error(
          s"Error while publishing to Kafka ${exception}")
    }
  })


  //flush & close
  producer.flush
  producer.close
}
