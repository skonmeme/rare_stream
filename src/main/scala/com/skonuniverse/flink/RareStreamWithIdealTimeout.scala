package com.skonuniverse.flink

import com.skonuniverse.flink.conifiguration.{FlinkConfig, KafkaConfig, ProducerConfig, RuntimeConfig}
import com.skonuniverse.flink.datatype.RareMessage
import com.skonuniverse.flink.specification.Flink
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object RareStreamWithIdealTimeout {
  def sink(stream: DataStream[RareMessage], producerConfig: ProducerConfig): Unit = {
    producerConfig.getProducer.foreach(producerWithTopic => {
      stream
          .addSink(producerWithTopic._2)
          .name("kafka-" + producerWithTopic._1 + "-producer-sink")
          .uid("kafka-" + producerWithTopic._1 + "-producer-sink-id")
    })
  }

  def main(args: Array[String]): Unit = {
    val config = RuntimeConfig.getConfig(args)
    val flinkConfig = FlinkConfig.getConfig(config)
    val consumerConfig = KafkaConfig.getConsumerConfig(config)
    val producerConfig = KafkaConfig.getProducerConfig(config)

    val eventProcessingInterval = 30 * 1000L

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    Flink.setEnvironment(env, flinkConfig)

    val mainStream = env
        .addSource(consumerConfig.getConsumer)
        .setParallelism(config.sourceParallelism)
        .name("kafka-consumers")
        .uid("kafka-consumers-id")
        .map(sr => sr._2)
        .name("process-per-topic")
        .uid("process-per-topic-id")
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
              .forBoundedOutOfOrderness[RareMessage](Duration.ofMillis(config.allowedLateness))
              .withTimestampAssigner(new SerializableTimestampAssigner[RareMessage] {
                override def extractTimestamp(element: RareMessage, recordTimestamp: Long): Long = element.eventTime.getTime
              })
              .withIdleness(Duration.ofMillis(config.fakeMessageInterval))
        )
        .keyBy(_.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(eventProcessingInterval)))
        .maxBy("value")
        .name("main-process")
        .uid("main-prcoess-id")

    sink(mainStream, producerConfig)

    env.execute("Rapid window processing of rare messages on Apache Flink")
  }
}
