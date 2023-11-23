package org.sunbird.obsrv.helper

import org.sunbird.obsrv.job.JDBCConnectorConfig
import org.sunbird.obsrv.model.{Actor, Context, Edata, JobMetric, MetricObject, Pdata}

case class MetricsHelper(config: JDBCConnectorConfig) extends BaseMetricHelper(config) {

  private def getObject(datasetId: String, dsVersion: Int) = {
    MetricObject(id = datasetId, `type` = "Dataset", ver = dsVersion)
  }

  def generate(datasetId: String, dsVersion: Int, edata: Edata) = {
    val `object` = getObject(datasetId, dsVersion)
    val actor = Actor(id = config.jobName, `type` = "SYSTEM")
    val pdata = Pdata(id = "Connectors", pid = config.jobName, ver = config.connectorVersion)
    val context = Context(env = config.env, pdata = pdata)
    val metric = JobMetric(actor = actor, context = context, `object` = `object`, edata = edata)
    this.sync(metric)
  }
}
