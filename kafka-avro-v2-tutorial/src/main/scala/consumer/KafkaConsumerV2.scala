package consumer

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import _root_.util.KafkaConstants.{BOOTSTRAP_SERVER, CONSUMER_GROUP, TOPIC}
import com.ww.User
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroDeserializerConfig}

import scala.collection.JavaConverters._

object KafkaConsumerV2 extends App {
  val logger = LoggerFactory.getLogger(KafkaConsumerV2.getClass)

  //Create consumer config
  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)
  properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")
  properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

  //create consumer
  val kafkaConsumer = new KafkaConsumer[String, User](properties)

  //subscribe consumer to the topic
  kafkaConsumer.subscribe(util.Arrays.asList(TOPIC))

  //poll for data
  while(true){

    val records: ConsumerRecords[String, User] = kafkaConsumer.poll(Duration.ofMillis(100))

    for(data: ConsumerRecord[String, User] <- records.iterator().asScala){
      val user: User = data.value()
      logger.info(s"Data received ${user.toString}")

    }

    //No need to explicitly commit offset as enable.auto.commit = true
    //kafkaConsumer.commitAsync()

  }
}
