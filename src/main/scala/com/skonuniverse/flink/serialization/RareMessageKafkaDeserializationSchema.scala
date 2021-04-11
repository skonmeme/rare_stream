package com.skonuniverse.flink.serialization

import com.skonuniverse.flink.datatype.RareMessage
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s._
import org.json4s.native.Serialization.read

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

class RareMessageKafkaDeserializationSchema[T <: RareMessage] extends KafkaDeserializationSchema[(String, T)] with LazyLogging {
  implicit val formats: DefaultFormats = new DefaultFormats {
     override def dateFormatter: SimpleDateFormat = {
       val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
       format.setTimeZone(DefaultFormats.UTC)
       format
    }
  }

  override def isEndOfStream(nextElement: (String, T)): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, T) = {
    try {
      val recordValue = new String(record.value(), StandardCharsets.UTF_8)
      (record.topic(), read[T](recordValue))
    } catch {
      case _: Throwable =>
        logger.warn("Invalid JSON string")
        null
    }
  }

  override def getProducedType: TypeInformation[(String, T)] = TypeInformation[(String, T)]
}
