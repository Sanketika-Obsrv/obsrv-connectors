package org.sunbird.obsrv.helper

import org.sunbird.obsrv.job.JDBCConnectorConfig
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetSourceConfig}
import org.sunbird.obsrv.model.{Edata, MetricLabel}
import org.sunbird.obsrv.util.JSONUtil

import java.util.UUID
import scala.collection.mutable

case class SingleEvent(dataset: String, event: Map[String, Any], syncts: Long, obsrv_meta: mutable.Map[String,Any])

object EventGenerator {

  def getBatchEvent(datasetId: String, record: String, dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig, extractionKey: String): String = {
    val event = Map(
      "id" -> UUID.randomUUID().toString,
      "dataset" -> datasetId,
      extractionKey -> List(JSONUtil.deserialize(record, classOf[Map[String, Any]])),
      "syncts" -> System.currentTimeMillis(),
      "obsrv_meta" -> getObsrvMeta(dsSourceConfig, config)
    )
    JSONUtil.serialize(event)
  }


  def getSingleEvent(datasetId: String, record: String, dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig): String = {
    val event = SingleEvent(
      datasetId,
      JSONUtil.deserialize(record, classOf[Map[String, Any]]),
      System.currentTimeMillis(),
      getObsrvMeta(dsSourceConfig, config)
    )
    JSONUtil.serialize(event)
  }

  def getObsrvMeta(dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig): mutable.Map[String,Any] = {
    val obsrvMeta = mutable.Map[String,Any]()
    obsrvMeta.put("syncts", System.currentTimeMillis())
    obsrvMeta.put("flags", Map())
    obsrvMeta.put("timespans", Map())
    obsrvMeta.put("error", Map())
    obsrvMeta.put("processingStartTime", System.currentTimeMillis())
    obsrvMeta.put("source", Map(
      "meta" -> Map(
        "id" -> dsSourceConfig.id,
        "connector_type" -> "jdbc",
        "version" -> config.connectorVersion,
        "entry_source" -> dsSourceConfig.connectorConfig.tableName
      ),
      "trace_id" -> UUID.randomUUID().toString
    ))
    obsrvMeta
  }

  def generateProcessingMetric(config: JDBCConnectorConfig, dataset: Dataset, batch: Int, eventCount: Long, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, eventProcessingTime: Long): Unit = {
    metrics.generate(
      dataset.id,
      edata = Edata(
        metric = Map(
          metrics.getMetricName("batch_count") -> batch,
          metrics.getMetricName("processed_event_count") -> eventCount,
          metrics.getMetricName("processing_time_in_ms") -> eventProcessingTime
        ),
        labels = List(
          MetricLabel("job", config.jobName),
          MetricLabel("databaseType", dsSourceConfig.connectorConfig.databaseType),
          MetricLabel("databaseName", dsSourceConfig.connectorConfig.databaseName),
          MetricLabel("tableName", dsSourceConfig.connectorConfig.tableName),
          MetricLabel("batchSize", String.valueOf(dsSourceConfig.connectorConfig.batchSize))
        )
      )
    )
  }

  def generateFetchMetric(config: JDBCConnectorConfig, dataset: Dataset, batch: Int, eventCount: Long, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, eventProcessingTime: Long): Unit = {
    metrics.generate(
      dataset.id,
      edata = Edata(
        metric = Map(
          metrics.getMetricName("batch_count") -> batch,
          metrics.getMetricName("fetched_event_count") -> eventCount,
          metrics.getMetricName("fetched_time_in_ms") -> eventProcessingTime
        ),
        labels = List(
          MetricLabel("job", config.jobName),
          MetricLabel("databaseType", dsSourceConfig.connectorConfig.databaseType),
          MetricLabel("databaseName", dsSourceConfig.connectorConfig.databaseName),
          MetricLabel("tableName", dsSourceConfig.connectorConfig.tableName),
          MetricLabel("batchSize", String.valueOf(dsSourceConfig.connectorConfig.batchSize))
        )
      )
    )
  }

  def generateErrorMetric(config: JDBCConnectorConfig, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, eventProcessingTime: Long, error: String, errorMessage: String): Unit = {
    metrics.generate(
      dsSourceConfig.datasetId,
      edata = Edata(
        metric = Map(),
        labels = List(
          MetricLabel("job", config.jobName),
          MetricLabel("databaseType", dsSourceConfig.connectorConfig.databaseType),
          MetricLabel("databaseName", dsSourceConfig.connectorConfig.databaseName),
          MetricLabel("tableName", dsSourceConfig.connectorConfig.tableName),
          MetricLabel("batchSize", String.valueOf(dsSourceConfig.connectorConfig.batchSize))
        ),
        err = error,
        errMsg = errorMessage
      )
    )
  }

}
