package org.sunbird.obsrv.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}

object JSONUtil {

  @transient private val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)
    .build() :: ClassTagExtensions

  mapper.setSerializationInclusion(Include.NON_NULL)

  def generateJSON(data : DataFrame) : Dataset[String] = {
    val columnTypes = data.dtypes.toMap
    data.select(data.columns.map(columnName => {
      val columnType = columnTypes(columnName)
      val castedColumn = columnType match {
        case "StringType" => col(columnName).cast(StringType)
        case "IntegerType" => col(columnName).cast(IntegerType)
        case "DoubleType" => col(columnName).cast(DoubleType)
        case "BooleanType" => col(columnName).cast(BooleanType)
        case "LongType" => col(columnName).cast(LongType)
        case "ShortType" => col(columnName).cast(ShortType)
        case "ByteType" => col(columnName).cast(ByteType)
        case "FloatType" => col(columnName).cast(FloatType)
        case "DecimalType" => col(columnName).cast(DecimalType.SYSTEM_DEFAULT)
        case "TimestampType" => col(columnName).cast(TimestampType)
        case "DateType" => col(columnName).cast(DateType)
        case _ => col(columnName) // No casting for unknown types
      }
      castedColumn.alias(columnName)
    }): _*).asInstanceOf[Dataset[String]].toJSON
  }

  def serialize(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }

}
