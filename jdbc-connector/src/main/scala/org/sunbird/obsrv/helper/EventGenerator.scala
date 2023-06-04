package org.sunbird.obsrv.helper

import org.sunbird.obsrv.util.JSONUtil

import java.util.UUID

case class BatchEvent(id: String, mid: String, dataset: String, events: List[Map[String, Any]], syncts: Long)

object EventGenerator {

  def getBatchEvent(datasetId: String, records: List[Map[String, Any]]): String = {
    val event = BatchEvent(UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      datasetId,
      records,
      System.currentTimeMillis()
    )
    JSONUtil.serialize(event)
  }

}
