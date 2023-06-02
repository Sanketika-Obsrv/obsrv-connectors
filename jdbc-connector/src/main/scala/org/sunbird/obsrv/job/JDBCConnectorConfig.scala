package org.sunbird.obsrv.job

import com.typesafe.config.ConfigFactory

class JDBCConnectorConfig (){

  private val config = ConfigFactory.load("application.conf").withFallback(ConfigFactory.systemEnvironment())

  val kafkaServerUrl: String = config.getString("kafka.broker-servers")
  val datasetId: String = config.getString("datasetId")
  val sparkMasterUrl: String = config.getString("spark.master.url")
  val jdbcConnectionRetry: Int = config.getInt("jdbc.connection.retry")
  val jdbcConnectionRetryDelay: Int = config.getInt("jdbc.connection.retryDelay")
}
