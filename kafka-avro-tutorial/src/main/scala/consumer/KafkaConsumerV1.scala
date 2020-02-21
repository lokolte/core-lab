package consumer

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import _root_.util.KafkaConstants.{BOOTSTRAP_SERVER, CONSUMER_GROUP, TOPIC}
import com.ww.UserV1
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import scala.collection.JavaConverters._

object KafkaConsumerV1 extends App {
  val logger = LoggerFactory.getLogger(KafkaConsumerV1.getClass)

  //Create consumer config
  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)
  properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")

  //create consumer
  val kafkaConsumer = new KafkaConsumer[String, UserV1](properties)

  //subscribe consumer to the topic
  kafkaConsumer.subscribe(util.Arrays.asList(TOPIC))

  //poll for data
  while(true){

    val records: ConsumerRecords[String, UserV1] = kafkaConsumer.poll(Duration.ofMillis(100))

    for(data: ConsumerRecord[String, UserV1] <- records.iterator().asScala)
      logger.info(data.toString)

    //No need to explicitly commit offset as enable.auto.commit = true
    //kafkaConsumer.commitAsync()

  }
}
