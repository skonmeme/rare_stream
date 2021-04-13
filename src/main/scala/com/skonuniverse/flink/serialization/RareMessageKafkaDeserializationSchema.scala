package com.skonuniverse.flink.serialization

import com.skonuniverse.flink.datatype.RareMessage
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s._
import org.json4s.native.Serialization.read

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import scala.reflect.classTag

class RareMessageKafkaDeserializationSchema extends KafkaDeserializationSchema[(String, RareMessage)] with LazyLogging {
  override def isEndOfStream(nextElement: (String, RareMessage)): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, RareMessage) = {
    implicit val formats: DefaultFormats = new DefaultFormats {
      override def dateFormatter: SimpleDateFormat = {
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        format.setTimeZone(DefaultFormats.UTC)
        format
      }
    }
    try {
      val recordValue = new String(record.value(), StandardCharsets.UTF_8)
      val c = read[RareMessage](recordValue)
      (record.topic(), read[RareMessage](recordValue))
    } catch {
      case e: Throwable =>
        logger.warn("Invalid JSON string: " + e)
        (record.topic(), null)
    }
  }

  override def getProducedType: TypeInformation[(String, RareMessage)] = {
    TypeExtractor.getForClass(classTag[(String, RareMessage)].runtimeClass).asInstanceOf[TypeInformation[(String, RareMessage)]]
  }
}
