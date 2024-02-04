package org.sunbird.obsrv.kafkaconnector.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.kafkaconnector.task.KafkaConnectorConfig
import org.sunbird.obsrv.kafkaconnector.transformer.DebeziumTransformer
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable.{Map => MMap}

class KafkaEventTransformerFunction(config: KafkaConnectorConfig)
                                   (implicit val stringTypeInfo: TypeInformation[String]) extends BaseProcessFunction[String, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[KafkaEventTransformerFunction])

  @transient private var debeziumTransformer: DebeziumTransformer = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    debeziumTransformer = new DebeziumTransformer()
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: String, context: ProcessFunction[String, String]#Context, metrics: Metrics): Unit = {
    val transformedEvent = debeziumTransformer.transform(event)
    transformedEvent.foreach {
      event => context.output(config.successTag(), event)
    }
  }

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successfulDebeziumTransformedCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }
}
