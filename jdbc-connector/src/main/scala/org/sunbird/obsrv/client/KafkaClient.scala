package org.sunbird.obsrv.client

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.sunbird.obsrv.job.{JDBCConnectorConfig, JDBCConnectorJob}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.Properties

class KafkaClient(config: JDBCConnectorConfig) extends Serializable {

  @transient private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)
  @transient private val producer = createProducer()
  private def getProducer: Producer[Long, String] = producer

  @throws[Exception]
  def send(event: String, topic: String): Unit = {
    try {
      getProducer.send(new ProducerRecord[Long, String](topic, event))
    } catch {
      case  ex: Exception => {
        logger.error("Error while sending data to kafka", ex)
      }
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
