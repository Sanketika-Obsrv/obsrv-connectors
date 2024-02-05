package org.sunbird.obsrv.kafkaconnector.spec

// import org.scalatest.Matchers.not.be
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.kafkaconnector.fixtures.EventFixture
import org.sunbird.obsrv.kafkaconnector.transformer.DebeziumTransformer

class DebeziumTransformerSpec extends org.scalatest.FlatSpec with org.scalatest.BeforeAndAfterAll {
  val transformer = new DebeziumTransformer()

  "DebeziumTransformer" should "transform a debezium event" in {
    val transformedEvent = transformer.transform(EventFixture.DEBEZIUM_CREATE_EVENT)
    // println(transformedEvent)
    transformedEvent shouldNot be (None)
    transformedEvent.map { event =>
      val eventData = JSONUtil.deserialize[Map[String, AnyRef]](event)
      // eventData.get("dataset_id") should be(Some("d4"))
      eventData.get("vehicleCode") should be(Some("HYUN-CRE-D6"))
    }
  }
}
