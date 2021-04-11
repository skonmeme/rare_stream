package com.skonuniverse.flink.conifiguration

import com.skonuniverse.flink.datatype.RareMessage
import com.skonuniverse.flink.serialization.{RareMessageKafkaDeserializationSchema, RareMessageKafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.common.errors.IllegalGenerationException

import java.util.Properties
import java.util.regex.Pattern
import scala.collection.JavaConverters._

sealed trait KafkaConfig[T <: RareMessage] {
  def bootstrapServer: String
  def topics: Either[Seq[String], Pattern]
  def properties: Properties

  //def getConsumer: FlinkKafkaConsumer[(String, T)] = null
  //def getProducer: Seq[FlinkKafkaProducer[T]] = null
}

case class ConsumerConfig[T <: RareMessage](bootstrapServer: String,
                                            topics: Either[Seq[String], Pattern],
                                            properties: Properties,
                                            groupId: String) extends KafkaConfig[T] {
  def getConsumer: FlinkKafkaConsumer[(String, T)] = {
    val fullProperties = new Properties(properties)
    fullProperties.put("bootstrap.servers", bootstrapServer)
    fullProperties.put("group.id", groupId)
    topics match {
      case Left(t) => new FlinkKafkaConsumer(t.asJava, new RareMessageKafkaDeserializationSchema[T], fullProperties)
      case Right(p) => new FlinkKafkaConsumer(p, new RareMessageKafkaDeserializationSchema[T], fullProperties)
    }
  }
}

case class ProducerConfig[T <: RareMessage](bootstrapServer: String,
                                            topics: Either[Seq[String], Pattern],
                                            properties: Properties,
                                            sematic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    extends KafkaConfig[T] {
  def getProducer: Seq[(String, FlinkKafkaProducer[T])] = {
    val fullProperties = new Properties(properties)
    fullProperties.put("bootstrap.servers", bootstrapServer)
    if (! fullProperties.containsKey("acks")) {
      fullProperties.put("acks", "1")
    }
    topics match {
      case Left(ts) => ts.map(t => (t, new FlinkKafkaProducer[T](t, new RareMessageKafkaSerializationSchema(t)[T], fullProperties, sematic)))
      case _ => throw new IllegalGenerationException("Topic pattern is not allowed for Producer!")
    }
  }
}

object KafkaConfig {
  def getConsumerConfig[T <: RareMessage](config: RuntimeConfig): ConsumerConfig[T] = {
    ConsumerConfig[T](
      bootstrapServer = config.bootstrapServer,
      topics = if (config.consumerTopics != null) Left(config.consumerTopics) else Right(config.consumerTopicPattern),
      properties = config.consumerProperties,
      groupId = config.groupId
    )
  }

  def getProducerConfig[T <: RareMessage](config: RuntimeConfig): ProducerConfig[T] = {
    ProducerConfig[T](
      bootstrapServer = config.brokerList,
      topics = Left(config.producerTopics),
      properties = config.producerProperties,
      sematic = config.sematic
    )
  }
}