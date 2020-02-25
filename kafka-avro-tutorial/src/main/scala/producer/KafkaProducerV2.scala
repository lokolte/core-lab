package producer

import java.util.Properties

import com.ww.{UserV1, UserV2}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import util.KafkaConstants._
import util.ProducerUtil._

object KafkaProducerV2 extends App {
  val logger = LoggerFactory.getLogger(KafkaProducerV2.getClass)

  //Set Kafka properties
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  props.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  //create the producer
  val producer = new KafkaProducer[String, UserV2](props)


  //create the producer record
  val userV2 : UserV2 = new UserV2("Test", "LTest", "111-111-1111")
  val record: ProducerRecord[String, UserV2] = new ProducerRecord(TOPIC, userV2)


  //publish the record
  producer.send(record, callback)


  //flush & close
  producer.flush
  producer.close
}
