package consumer

import java.time.Duration
import java.util.Properties

import util.KafkaConstants._
import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object KafkaConsumerEx extends App {


  val logger = LoggerFactory.getLogger(KafkaConsumerEx.getClass)

  //Create consumer config
  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)


  //create consumer
  val kafkaConsumer = new KafkaConsumer[String, String](properties)

  //subscribe consumer to the topic
  kafkaConsumer.subscribe(util.Arrays.asList(TOPIC))

  //poll for data
  while(true){

    val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMillis(100))

    for(data: ConsumerRecord[String, String] <- records.iterator())
      logger.info(data.toString)

    //No need to explicitly commit offset as enable.auto.commit = true
    //kafkaConsumer.commitAsync()

  }
}
