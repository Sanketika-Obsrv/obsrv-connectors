package org.sunbird.obsrv.client

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.sunbird.obsrv.job.JDBCConnectorConfig

import java.util.Properties

class KafkaClient(config: JDBCConnectorConfig) {
  private val producer = createProducer()
  protected def getProducer: Producer[Long, String] = producer

  @throws[Exception]
  def send(event: String, topic: String): Unit = {
      getProducer.send(new ProducerRecord[Long, String](topic, event))
  }

  private def createProducer(): KafkaProducer[Long, String] = {
    new KafkaProducer[Long, String](new Properties() {
      {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaServerUrl)
        put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaClientProducer")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      }
    })
  }
}
