package com.skonuniverse.flink.configuration

import com.skonuniverse.flink.datatype.RareMessage
import com.skonuniverse.flink.serialization.{RareMessageKafkaDeserializationSchema, RareMessageKafkaSerializationSchema}
import com.skonuniverse.flink.specification.Kafka
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.common.errors.IllegalGenerationException

import java.util.Properties
import java.util.regex.Pattern
import scala.collection.JavaConverters._

sealed trait KafkaConfig {
  def bootstrapServer: String
  def topics: Either[Seq[String], Pattern]
  def properties: Properties
}

case class ConsumerConfig(bootstrapServer: String,
                          topics: Either[Seq[String], Pattern],
                          properties: Properties,
                          groupId: String,
                          consumingOffset: String = "latest",
                          partitionDiscoveryInterval: Long = 60 * 1000L) extends KafkaConfig {
  def getConsumer: FlinkKafkaConsumer[(String, RareMessage)] = {
    val fullProperties = new Properties(properties)
    fullProperties.put("bootstrap.servers", bootstrapServer)
    fullProperties.put("group.id", groupId)
    val consumer = topics match {
      case Left(t) => new FlinkKafkaConsumer(t.asJava, new RareMessageKafkaDeserializationSchema, fullProperties)
      case Right(p) => {
        fullProperties.put("flink.partition-discovery.interval-millis", partitionDiscoveryInterval.toString)
        new FlinkKafkaConsumer(p, new RareMessageKafkaDeserializationSchema, fullProperties)
      }
    }
    Kafka.consumerWithOffset(consumer, consumingOffset)
  }
}

case class ProducerConfig(bootstrapServer: String,
                          topics: Either[Seq[String], Pattern],
                          properties: Properties,
                          sematic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
    extends KafkaConfig {
  def getProducer: Seq[(String, FlinkKafkaProducer[RareMessage])] = {
    val fullProperties = new Properties(properties)
    fullProperties.put("bootstrap.servers", bootstrapServer)
    if (! fullProperties.containsKey("acks")) {
      fullProperties.put("acks", "1")
    }
    topics match {
      case Left(ts) => ts.map(t => (t, new FlinkKafkaProducer(t, new RareMessageKafkaSerializationSchema(t), fullProperties, sematic)))
      case _ => throw new IllegalGenerationException("Topic pattern is not allowed for Producer!")
    }
  }
}

object KafkaConfig {
  def getConsumerConfig(config: RuntimeConfig): ConsumerConfig = {
    ConsumerConfig(
      bootstrapServer = config.bootstrapServer,
      topics = if (config.consumerTopics != null) Left(config.consumerTopics) else Right(config.consumerTopicPattern),
      properties = config.consumerProperties,
      groupId = config.groupId,
      consumingOffset = config.consumingOffset,
      partitionDiscoveryInterval = config.partitionDiscoveryInterval
    )
  }

  def getProducerConfig(config: RuntimeConfig): ProducerConfig = {
    ProducerConfig(
      bootstrapServer = config.brokerList,
      topics = Left(config.producerTopics),
      properties = config.producerProperties,
      sematic = config.sematic
    )
  }
}