package producer

import java.util.Properties

import util.KafkaConstants._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object KafkaProducerWithKeysEx extends App {

  val logger = LoggerFactory.getLogger(KafkaProducerWithKeysEx.getClass)

  //Set Kafka properties
  val props: Properties = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

//create the producer
  val producer = new KafkaProducer[String, String](props)

  for(i <- 0 to 10) {

    val value = s"hello-world${i}"
    val key = s"id_${i}"

    logger.info(s"***Key: ${key}")
    //create the producer record
    val record: ProducerRecord[String, String] = new ProducerRecord(TOPIC, key, value)

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
        }).get()
    }
//4,6,9,10 => partition 0

  //flush & close
  producer.flush
  producer.close
}
