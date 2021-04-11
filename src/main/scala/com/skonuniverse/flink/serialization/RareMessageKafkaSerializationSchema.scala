package com.skonuniverse.flink.serialization

import com.skonuniverse.flink.datatype.RareMessage
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import java.lang
import java.nio.charset.{Charset, StandardCharsets}
import java.text.SimpleDateFormat

class RareMessageKafkaSerializationSchema[T <: RareMessage](topic: String, charset: Charset = StandardCharsets.UTF_8)
    extends KafkaSerializationSchema[T] with LazyLogging {
  implicit val formats: DefaultFormats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      format.setTimeZone(DefaultFormats.UTC)
      format
    }
  }
  override def serialize(element: T, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    try {
      val recordMessage = write(element)
      new ProducerRecord[Array[Byte], Array[Byte]](topic, recordMessage.getBytes(charset))
    } catch {
      case e: Throwable =>
        logger.warn("JSON string genration is failed: " + e)
        null
    }
  }
}
