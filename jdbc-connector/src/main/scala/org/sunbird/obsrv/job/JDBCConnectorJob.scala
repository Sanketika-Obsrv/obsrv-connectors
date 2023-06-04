package org.sunbird.obsrv.job

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.sunbird.obsrv.client.KafkaClient
import org.sunbird.obsrv.helper.EventGenerator
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.util.JSONUtil

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Try}

object JDBCConnectorJob {

  private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  def main(args: Array[String]): Unit = {
    val config = new JDBCConnectorConfig()
    val kafkaClient = new KafkaClient(config)
    val dataset = DatasetRegistry.getDataset(config.datasetId).get
    val dsSourceConfig = DatasetRegistry.getDatasetSourceConfigById(config.datasetId)
    val connectorConfig = dsSourceConfig.connectorConfig
    val connectorStats = dsSourceConfig.connectorStats
    var eventCount = 0
    Console.println("dataset " + dataset.datasetConfig)
    Console.println("connector " + connectorConfig)
    Console.println("connector " + connectorStats)
    //val dataset =  DatasetConfig("", "created_on", "spark.test", None, None,None,None)
//    val connectorConfig = ConnectorConfig("localhost:9092", "test", "postgres",
//      "postgres", "localhost", 5432, "postgres", "user_table",
//      10, 5, "postgresql", Timestamp.valueOf("2023-06-02 18:36:02.264"))

    val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val jdbcUrl = s"jdbc:${connectorConfig.jdbcDatabaseType}://${connectorConfig.jdbcHost}:${connectorConfig.jdbcPort}/${connectorConfig.jdbcDatabase}"
    val delayMs = (60 * 1000) / connectorConfig.jdbcBatchesPerMinute
    var retryCount = 0
    var batch = 0

    breakable {
      while (true) {
        logger.info(s"Pulling batch: ${batch + 1}")
        val offset = batch * connectorConfig.jdbcBatchSize
        var query: String = null
        val readStartTime = System.currentTimeMillis()
        if (connectorStats.lastFetchTimestamp == null) {
          query = s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
        } else {
          query = s"SELECT * FROM ${connectorConfig.jdbcDatabaseTable} WHERE ${dataset.datasetConfig.tsKey} > '${connectorStats.lastFetchTimestamp}' LIMIT ${connectorConfig.jdbcBatchSize} OFFSET $offset"
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
              logger.error(s"Database Connection failed. Retrying (${retryCount + 1}/${config.jdbcConnectionRetry})...", exception)
              Thread.sleep(config.jdbcConnectionRetryDelay)
              retryCount += 1
              if (retryCount == config.jdbcConnectionRetry) break;
            case util.Success(df) =>
              data = df
          }
        }

        if (data.collect().length == 0) {
          logger.info("All the records are pulled...")
          break
        }

        val batchReadTime = System.currentTimeMillis() - readStartTime
        data.show()
        val records = JSONUtil.parseRecords(data)
        kafkaClient.send(EventGenerator.getBatchEvent(config.datasetId, records), "spark.test")

        val lastRowTimestamp = data.orderBy(data(dataset.datasetConfig.tsKey).desc).first().getAs[Timestamp](dataset.datasetConfig.tsKey)
        DatasetRegistry.updateConnectorStats(config.datasetId, lastRowTimestamp, data.collect().length, batchReadTime)
        eventCount += data.collect().length
        logger.info(s"Batch ${batch + 1} is processed successfully :: Number of records pulled: ${data.collect().length} :: Batch Read Time: ${batchReadTime}")

        // Sleep for the specified delay between batches
        Thread.sleep(delayMs)
        batch += 1
      }
    }

    logger.info(s"Total number of records are pulled: ${eventCount}")
    spark.stop()
  }

}

