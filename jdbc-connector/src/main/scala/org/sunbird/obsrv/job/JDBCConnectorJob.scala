package org.sunbird.obsrv.job

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.client.KafkaClient
import org.sunbird.obsrv.helper.EventGenerator
import org.sunbird.obsrv.model.DatasetModels.{ConnectorConfig, DatasetConfig}
import org.sunbird.obsrv.util.JSONUtil

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Breaks.break
import scala.util.{Failure, Try}

object JDBCConnectorJob {

  val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  def main(args: Array[String]): Unit = {
    val config = new JDBCConnectorConfig()
    val kafkaClient = new KafkaClient(config)
    //val dataset = DatasetRegistry.getDataset(config.datasetId).get
    //    val connectorConfig = DatasetRegistry.getDatasetSourceConfigById(config.datasetId).connectorConfig
    val dataset =  DatasetConfig("", "created_on", "spark.test", None, None,None,None)
    val connectorConfig = ConnectorConfig("localhost:9092", "test", "postgres",
      "postgres", "localhost", 5432, "postgres", "user_table",
      10, 5, "postgresql", Timestamp.valueOf("2023-06-02 18:36:02.264"))

    val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val jdbcUrl = s"jdbc:${connectorConfig.jdbcDatabaseType}://${connectorConfig.jdbcHost}:${connectorConfig.jdbcPort}/${connectorConfig.jdbcDatabase}"
    val delayMs = (60 * 1000) / connectorConfig.jdbcBatchesPerMinute

    var retryCount = 0


    for (batch <- 0 until connectorConfig.jdbcBatchesPerMinute) {
      logger.info(s"Pulling batch: ${batch+1} :: Start Time: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())}")
      val offset = batch * connectorConfig.jdbcBatchSize
      var query: String = null
      if(StringUtils.isEmpty(connectorConfig.lastFetchTimestamp)) {
        query = s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
      } else {
        query = s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} WHERE ${dataset.tsKey} > '${connectorConfig.lastFetchTimestamp}' LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
      }
      var data: DataFrame = null
      while (retryCount < config.jdbcConnectionRetry && data == null) {
        val connectionResult = Try {
          spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("user", connectorConfig.jdbcUser)
            .option("password", connectorConfig.jdbcPassword)
            .option("query", query)
            .load()
        }

        connectionResult match {
          case Failure(exception) =>
            logger.info(s"Database Connection failed. Retrying (${retryCount + 1}/${config.jdbcConnectionRetry})...")
            logger.error("exception " + exception.getMessage)
            exception.printStackTrace()
            Thread.sleep(config.jdbcConnectionRetryDelay)
            retryCount += 1
            if (retryCount == config.jdbcConnectionRetry) break;
          case util.Success(df) =>
            data = df
        }
      }

      if (data == null) {
        logger.info("Failed to establish connection after maximum retries.")
      } else {
        data.show()
        val jsonDataSet = JSONUtil.generateJSON(data)
        kafkaClient.send(EventGenerator.getBatchEvent(config.datasetId, jsonDataSet.collect().toList), "spark.test")
        val lastRowTimestamp = data.orderBy(data(dataset.tsKey).desc).first().getAs[String](dataset.tsKey)
        // update dataset registry
        logger.info(s"Batch ${batch+1} is processed successfully :: End Time: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())}")
        // Sleep for the specified delay between batches
        Thread.sleep(delayMs)
      }
    }

    spark.stop()
  }


}

