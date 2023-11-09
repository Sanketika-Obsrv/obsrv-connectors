package org.sunbird.obsrv.job

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.client.KafkaClient
import org.sunbird.obsrv.helper.ConnectorHelper
import org.sunbird.obsrv.model.DatasetModels
import org.sunbird.obsrv.registry.DatasetRegistry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import scala.util.control.Breaks.{break, breakable}

object JDBCConnectorJob extends Serializable {

  private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  def main(args: Array[String]): Unit = {
    val appConfig = ConfigFactory.load("application.conf").withFallback(ConfigFactory.systemEnvironment())
    val config = new JDBCConnectorConfig(appConfig, args)
    val kafkaClient = new KafkaClient(config)
    val helper = new ConnectorHelper(config)
    val dsSourceConfigList =  DatasetRegistry.getDatasetSourceConfig()

     val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val filteredDSSourceConfigList = dsSourceConfigList.map { configList =>
      configList.filter(_.connectorType.equalsIgnoreCase("jdbc"))
    }.getOrElse(List())

    logger.info(s"Total no of datasets to be processed: ${filteredDSSourceConfigList.size}")

    filteredDSSourceConfigList.map {
        dataSourceConfig =>
          processTask(config, kafkaClient, helper, spark, dataSourceConfig)
    }

    spark.stop()
  }

  private def processTask(config: JDBCConnectorConfig, kafkaClient: KafkaClient, helper: ConnectorHelper, spark: SparkSession, dataSourceConfig: DatasetModels.DatasetSourceConfig) = {
    logger.info(s"Started processing dataset: ${dataSourceConfig.datasetId}")
    val dataset = DatasetRegistry.getDataset(dataSourceConfig.datasetId).get
    var batch = 0
    var eventCount = 0
    breakable {
      while (true) {
        val (data: DataFrame, batchReadTime: Long) = helper.pullRecords(spark, dataSourceConfig, dataset, batch)
        batch += 1
        if (data.count == 0) {
          DatasetRegistry.updateConnectorAvgBatchReadTime(dataSourceConfig.datasetId, batchReadTime / batch)
          break
        } else {
          helper.processRecords(config, kafkaClient, dataset, batch, data, batchReadTime, dataSourceConfig)
          eventCount += data.collect().length
        }
      }
    }
    logger.info(s"Completed processing dataset: ${dataSourceConfig.datasetId} :: Total number of records are pulled: $eventCount")
    dataSourceConfig
  }
}

