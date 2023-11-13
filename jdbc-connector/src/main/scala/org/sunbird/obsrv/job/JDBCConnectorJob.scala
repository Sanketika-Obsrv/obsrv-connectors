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
    val appConfig = ConfigFactory.load("jdbc-connector.conf").withFallback(ConfigFactory.systemEnvironment())
    val config = new JDBCConnectorConfig(appConfig, args)
    val kafkaClient = new KafkaClient(config)
    val helper = new ConnectorHelper(config)
    val dsSourceConfigList =  DatasetRegistry.getDatasetSourceConfig()

     val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val filteredDSSourceConfigList = dsSourceConfigList.map { configList =>
      configList.filter(config => config.connectorType.equalsIgnoreCase("jdbc") && config.status.equalsIgnoreCase("active"))
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
    var batch: Int = 0
    var eventCount: Long = 0
    breakable {
      while (true) {
        val (data: DataFrame, batchReadTime: Long) = helper.pullRecords(spark, dataSourceConfig, dataset, batch)
        batch += 1

        if (data.count == 0 || validateMaxSize(eventCount, config.eventMaxLimit)) {
          DatasetRegistry.updateConnectorAvgBatchReadTime(dataSourceConfig.datasetId, batchReadTime / batch)
          logger.info("Updating the metrics to the database...")
          break
        } else {
          helper.processRecords(config, kafkaClient, dataset, batch, data, batchReadTime, dataSourceConfig)
          eventCount += data.count()
        }
      }
    }
    logger.info(s"Completed processing dataset: ${dataSourceConfig.datasetId} :: Total number of records are pulled: $eventCount")
    dataSourceConfig
  }

  private def validateMaxSize(eventCount: Long, maxLimit: Long): Boolean = {
     if (maxLimit == -1) {
       false
     } else if (eventCount > maxLimit) {
       logger.info(s"Max fetch limit is reached, stopped fetching :: event count: ${eventCount} :: max limit: ${maxLimit}")
       true
     } else false
  }
}

