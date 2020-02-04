package com.flink.typeschema

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

/**
  * Byte Array serialization schema used for kafka messaging.
  */
class ByteArraySchema extends DeserializationSchema[Array[Byte]] with SerializationSchema[Array[Byte]] {

  override def deserialize(message: Array[Byte]): Array[Byte] = message

  override def isEndOfStream(nextElement: Array[Byte]): Boolean = false

  override def serialize(element: Array[Byte]): Array[Byte] = element

  override def getProducedType: TypeInformation[Array[Byte]] = TypeExtractor.getForClass(classOf[Array[Byte]])
}
