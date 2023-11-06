package org.sunbird.obsrv.job

import com.typesafe.config.Config

class JDBCConnectorConfig (config: Config, args: Array[String]){

  val kafkaServerUrl: String = config.getString("kafka.broker-servers")
  val sparkMasterUrl: String = config.getString("spark.master.url")
  val jdbcConnectionRetry: Int = config.getInt("jdbc.connection.retry")
  val jdbcConnectionRetryDelay: Int = config.getInt("jdbc.connection.retryDelay")
  val ingestTopic: String = config.getString("kafka.topic.ingest")
  val connectorVersion: String = config.getString("connector.version")

}
