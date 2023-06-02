package org.sunbird.obsrv.helper

import org.sunbird.obsrv.util.JSONUtil

import java.util.UUID

case class BatchEvent(id: String, mid: String, dataset: String, events: List[String], syncts: Long)

object EventGenerator {

  def getBatchEvent(datasetId: String, events: List[String]): String = {
    val event = BatchEvent(UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      datasetId,
      events,
      System.currentTimeMillis()
    )
    JSONUtil.serialize(event)
  }

}
