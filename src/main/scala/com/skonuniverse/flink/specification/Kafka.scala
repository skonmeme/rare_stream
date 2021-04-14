package com.skonuniverse.flink.specification

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Kafka {
  def consumerWithOffset[T](consumer: FlinkKafkaConsumer[T], offsetType: String): FlinkKafkaConsumer[T] = {
    offsetType match {
      case "earliest" => consumer.setStartFromEarliest()
      case "latest" => consumer.setStartFromLatest()
      case t: String => try {
        val timestamp = t.toLong
        consumer.setStartFromTimestamp(timestamp)
      } catch {
        case _: Throwable => consumer.setStartFromGroupOffsets()
      }
      case _ => consumer.setStartFromGroupOffsets()
    }
    consumer
  }
}
