package com.skonuniverse.flink

import com.skonuniverse.flink.conifiguration.{FlinkConfig, KafkaConfig, ProducerConfig, RuntimeConfig}
import com.skonuniverse.flink.datatype.RareMessage
import com.skonuniverse.flink.function.EventTimeProcess
import com.skonuniverse.flink.source.RareMessageSource
import com.skonuniverse.flink.specification.Flink
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object RareStreamWithFakeMessage {
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
    //val consumerConfig = KafkaConfig.getConsumerConfig(config)
    val producerConfig = KafkaConfig.getProducerConfig(config)

    val eventProcessingInterval = 10 * 1000L

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    Flink.setEnvironment(env, flinkConfig)

    val rareMessageStream = env
        //.addSource(consumerConfig.getConsumer)
        //.name("kafka-consumers")
        //.uid("kafka-consumers-id")
        //.map(sr => sr._2)
        //.name("process-per-topic")
        //.uid("process-per-topic-id")
        .addSource(new RareMessageSource)
        .setParallelism(config.sourceParallelism)
        .name("rare-messages-source")
        .uid("rare-messages-source-id")
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
              .forBoundedOutOfOrderness[RareMessage](Duration.ofMillis(config.allowedLateness))
              .withTimestampAssigner(new SerializableTimestampAssigner[RareMessage] {
                override def extractTimestamp(element: RareMessage, recordTimestamp: Long): Long = element.eventTime.getTime
              })
        ) // this operator is not necessary, because assignTimestampsAndWatermarks is called in EventTimeProcess routine once again.

    val eventTimeProcessedStream = EventTimeProcess.getStream(rareMessageStream, env, config)

    val mainStream = eventTimeProcessedStream
        .keyBy(_.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(eventProcessingInterval)))
        .reduce((v1, v2) => {
          if (v1.value < v2.value) v2
          else v1
        })
        .name("main-process")
        .uid("main-prcoess-id")

    mainStream.print()
    //sink(mainStream, producerConfig)

    env.execute("Rapid window processing of rare messages on Apache Flink")
  }
}
