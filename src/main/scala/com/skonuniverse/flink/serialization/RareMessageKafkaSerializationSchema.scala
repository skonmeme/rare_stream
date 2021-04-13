package com.skonuniverse.flink.serialization

import com.skonuniverse.flink.datatype.RareMessage
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import java.lang
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

class RareMessageKafkaSerializationSchema(topic: String)
    extends KafkaSerializationSchema[RareMessage] with LazyLogging {
  override def serialize(element: RareMessage, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    implicit val formats: DefaultFormats = new DefaultFormats {
      override def dateFormatter: SimpleDateFormat = {
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        format.setTimeZone(DefaultFormats.UTC)
        format
      }
    }
    try {
      val recordMessage = write(element)
      new ProducerRecord[Array[Byte], Array[Byte]](topic, recordMessage.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Throwable =>
        logger.warn("JSON string genration is failed: " + e)
        null
    }
  }
}
