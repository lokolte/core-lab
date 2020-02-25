package producer

import java.util.Properties

import com.ww.UserV1
import io.confluent.kafka.serializers.KafkaAvroSerializer
import util.KafkaConstants._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import util.ProducerUtil._

object KafkaProducerV1 extends App {
  val logger = LoggerFactory.getLogger(KafkaProducerV1.getClass)

  //Set Kafka properties
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  props.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  //create the producer
  val producer = new KafkaProducer[String, UserV1](props)


  //create the producer record
  val userV1 : UserV1 = new UserV1("Test", "LTest")
  val record: ProducerRecord[String, UserV1] = new ProducerRecord(TOPIC, userV1)

  //publish the record
  producer.send(record, callback)

  //flush & close
  producer.flush
  producer.close
}
