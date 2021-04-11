package com.skonuniverse.flink

import com.skonuniverse.flink.conifiguration.{FlinkConfig, KafkaConfig, ProducerConfig, RuntimeConfig}
import com.skonuniverse.flink.datatype.{DefaultRareMessage, RareMessage}
import com.skonuniverse.flink.function.EventTimeProcess
import com.skonuniverse.flink.source.FakeMessageSource
import com.skonuniverse.flink.specification.Flink
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

class RareStreamWithFakeMessage {
  def sink[T <: RareMessage](stream: DataStream[T], producerConfig: ProducerConfig[T]): Unit = {
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
    val consumerConfig = KafkaConfig.getConsumerConfig[DefaultRareMessage](config)
    val producerConfig = KafkaConfig.getProducerConfig[DefaultRareMessage](config)

    val eventProcessingInterval = 30 * 1000L

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    Flink.setEnvironment(env, flinkConfig)

    val fakeStream = env
        .addSource(new FakeMessageSource[DefaultRareMessage](config.fakeMessageInterval))
        .setParallelism(config.sourceParallelism)
        .name("fake-message-source")
        .uid("fake-message-source-id")

    val mainStream = env
        .addSource(consumerConfig.getConsumer)
        .setParallelism(config.sourceParallelism)
        .name("kafka-consumers")
        .uid("kafka-consumers-id")
        .map(sr => sr._2)
        .name("process-per-topic")
        .uid("process-per-topic-id")
        .connect(fakeStream)
        .process(new EventTimeProcess[DefaultRareMessage])
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
              .forBoundedOutOfOrderness[DefaultRareMessage](Duration.ofMillis(config.allowedLateness))
              .withTimestampAssigner(new SerializableTimestampAssigner[DefaultRareMessage] {
                override def extractTimestamp(element: DefaultRareMessage, recordTimestamp: Long): Long = element.eventTime.getTime
              })
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
