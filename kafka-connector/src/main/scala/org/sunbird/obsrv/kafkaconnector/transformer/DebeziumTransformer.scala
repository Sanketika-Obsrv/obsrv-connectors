package org.sunbird.obsrv.kafkaconnector.transformer

import com.fasterxml.jackson.databind.JsonMappingException
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset

case class DebeziumEvent(schema: Map[String, AnyRef], payload: DebeziumPayload)
case class DebeziumPayload(before: Map[String, AnyRef], after: Map[String, AnyRef], source: Map[String, AnyRef], op: String, ts_ms: Long)

class DebeziumTransformer {

  private[this] val logger = LoggerFactory.getLogger(classOf[DebeziumTransformer])

  def transform(event: String): Option[String] = {
    try {
      val debeziumEvent = JSONUtil.deserialize[DebeziumEvent](event)
      val debeziumPayload = debeziumEvent.payload
      Some(JSONUtil.serialize(debeziumPayload.after))
    } catch {
      case jme: JsonMappingException =>
        logger.error(s"DebeziumTransformer | Error during DebeziumEvent deserialization | ErrorMessage=${jme.getMessage}")
        None
    }
  }
}
