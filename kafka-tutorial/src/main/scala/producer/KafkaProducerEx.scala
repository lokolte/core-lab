package producer

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import util.KafkaConstants._


object KafkaProducerEx extends App {

  val logger = LoggerFactory.getLogger(KafkaProducerEx.getClass)

  //Set Kafka properties
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  //create the producer
  val producer = new KafkaProducer[String, String](props)

  for(i <- 0 to 10) {

    //create the producer record
    val record: ProducerRecord[String, String] = new ProducerRecord(TOPIC, s"hello-world${i}")

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
    }

  //flush & close
  producer.flush
  producer.close
}
