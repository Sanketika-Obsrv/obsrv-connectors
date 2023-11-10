package org.sunbird.obsrv.client

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.{LogManager, Logger}
import org.sunbird.obsrv.job.JDBCConnectorConfig

import java.util.Properties

class KafkaClient(config: JDBCConnectorConfig) extends Serializable {

  @transient private val logger: Logger = LogManager.getLogger(classOf[KafkaClient])

  @throws[Exception]
  def send(event: String, topic: String): Unit = {
    val producer = createProducer()
    try {
      producer.send(new ProducerRecord[Long, String](topic, event))
    } catch {
      case ex: Exception =>
        logger.error("Error while sending data to kafka", ex.getMessage)
    } finally {
      producer.close()
    }
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
