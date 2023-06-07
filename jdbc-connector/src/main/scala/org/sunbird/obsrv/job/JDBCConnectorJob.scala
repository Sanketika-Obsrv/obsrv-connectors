package org.sunbird.obsrv.job

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.client.KafkaClient
import org.sunbird.obsrv.helper.ConnectorHelper
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.util.control.Breaks.{break, breakable}

object JDBCConnectorJob {

  private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  def main(args: Array[String]): Unit = {
    val appConfig = ConfigFactory.load("application.conf").withFallback(ConfigFactory.systemEnvironment())
    val config = new JDBCConnectorConfig(appConfig, args)
    val kafkaClient = new KafkaClient(config)
    val dataset = DatasetRegistry.getDataset(config.datasetId).get
    val dsSourceConfig = DatasetRegistry.getDatasetSourceConfigById(config.datasetId)
    val helper = new ConnectorHelper(config)
    val delayMs = (60 * 1000) / dsSourceConfig.connectorConfig.jdbcBatchesPerMinute
    var batch = 0
    var eventCount = 0

    val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    breakable {
      while (true) {
        val (data: DataFrame, batchReadTime: Long) = helper.pullRecords(spark, dsSourceConfig, dataset, batch)
        batch += 1
        if (data.collect().length == 0) {
          DatasetRegistry.updateConnectorAvgBatchReadTime(config.datasetId, batchReadTime/batch)
          break
        } else {
          helper.processRecords(config, kafkaClient, dataset, batch, data, batchReadTime)
          eventCount += data.collect().length
          // Sleep for the specified delay between batches
          Thread.sleep(delayMs)
        }
      }
    }

    logger.info(s"Total number of records are pulled: $eventCount")
    spark.stop()
  }
}

