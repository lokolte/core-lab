package util

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import producer.KafkaProducerV1.logger

object ProducerUtil {

  val callback: Callback =  new Callback {
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
  }
}
