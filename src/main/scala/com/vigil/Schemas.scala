package com.vigil

import org.apache.spark.sql.Encoders

object Schemas {
  case class KeyValue(Key: Int, Value: Int)
  var keyValueSchemaEncoder = Encoders.product[KeyValue]
  var keyValueSchema = Encoders.product[KeyValue].schema
}
