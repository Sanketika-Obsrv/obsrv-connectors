package org.sunbird.obsrv.helper

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.client.KafkaClient
import org.sunbird.obsrv.job.JDBCConnectorConfig
import org.sunbird.obsrv.model.DatasetModels
import org.sunbird.obsrv.model.DatasetModels.{ConnectorConfig, ConnectorStats, Dataset, DatasetSourceConfig}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.util.JSONUtil

import java.sql.Timestamp
import scala.util.control.Breaks.break
import scala.util.{Failure, Try}

class ConnectorHelper(config: JDBCConnectorConfig) {

 private final val logger: Logger = LogManager.getLogger(getClass)

  def pullRecords(spark: SparkSession, dsSourceConfig: DatasetSourceConfig, dataset: Dataset, batch: Int): (DataFrame, Long) = {
    val connectorConfig = dsSourceConfig.connectorConfig
    val connectorStats = dsSourceConfig.connectorStats
    val jdbcUrl = s"jdbc:${connectorConfig.jdbcDatabaseType}://${connectorConfig.jdbcHost}:${connectorConfig.jdbcPort}/${connectorConfig.jdbcDatabase}"
    val query: String = getQuery(connectorStats, connectorConfig, dataset, batch * connectorConfig.jdbcBatchSize)
    var data: DataFrame = null
    var batchReadTime: Long = 0
    var retryCount = 0

    logger.info(s"Started pulling batch: ${batch + 1}")

    while (retryCount < config.jdbcConnectionRetry && data == null) {
      val connectionResult = Try {
        val readStartTime = System.currentTimeMillis()
        val result = spark.read.format("jdbc")
          .option("url", jdbcUrl)
          .option("user", connectorConfig.jdbcUser)
          .option("password", connectorConfig.jdbcPassword)
          .option("query", query)
          .load()
        batchReadTime = System.currentTimeMillis() - readStartTime
        result
      }

      connectionResult match {
        case Failure(exception) =>
          logger.error(s"Database Connection failed. Retrying (${retryCount + 1}/${config.jdbcConnectionRetry})...", exception)
          Thread.sleep(config.jdbcConnectionRetryDelay)
          retryCount += 1
          if (retryCount == config.jdbcConnectionRetry) break;
        case util.Success(df) =>
          data = df
      }
    }
    (data, batchReadTime)
  }

  def processRecords(config: JDBCConnectorConfig, kafkaClient: KafkaClient, dataset: DatasetModels.Dataset, batch: Int, data: DataFrame, batchReadTime: Long): Unit = {
    val records = JSONUtil.parseRecords(data)
    val lastRowTimestamp = data.orderBy(data(dataset.datasetConfig.tsKey).desc).first().getAs[Timestamp](dataset.datasetConfig.tsKey)
    kafkaClient.send(EventGenerator.getBatchEvent(config.datasetId, records), "spark.test")
    DatasetRegistry.updateConnectorStats(config.datasetId, lastRowTimestamp, data.collect().length, batchReadTime)
    logger.info(s"Batch ${batch + 1} is processed successfully :: Number of records pulled: ${data.collect().length} :: Batch Read Time: $batchReadTime")
  }

  def getQuery(connectorStats: ConnectorStats, connectorConfig: ConnectorConfig, dataset: Dataset, offset: Int): String = {
    if (connectorStats.lastFetchTimestamp == null) {
      s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} ORDER BY ${dataset.datasetConfig.tsKey} LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
    } else {
      s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} WHERE ${dataset.datasetConfig.tsKey} > '${connectorStats.lastFetchTimestamp}' ORDER BY ${dataset.datasetConfig.tsKey} LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
    }
  }

}
