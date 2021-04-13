package com.skonuniverse.flink

import com.skonuniverse.flink.conifiguration.{FlinkConfig, KafkaConfig, ProducerConfig, RuntimeConfig}
import com.skonuniverse.flink.datatype.RareMessage
import com.skonuniverse.flink.function.EventTimeProcess
import com.skonuniverse.flink.source.{FakeMessageSource, RareMessageSource}
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

  class Partitions(paralellism: Int) extends Serializable {
    var partition = 0

    def next: Int = {
      partition = (partition + 1) % paralellism
      partition
    }
  }

  def main(args: Array[String]): Unit = {
    val config = RuntimeConfig.getConfig(args)
    val flinkConfig = FlinkConfig.getConfig(config)
    //val consumerConfig = KafkaConfig.getConsumerConfig(config)
    val producerConfig = KafkaConfig.getProducerConfig(config)

    val eventProcessingInterval = 5 * 1000L
    val parition = new Partitions(config.sourceParallelism)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    Flink.setEnvironment(env, flinkConfig)

    val fakeStream = env
        .addSource(new FakeMessageSource(config.fakeMessageInterval, config.sourceParallelism))
        .setParallelism(config.sourceParallelism)
        .name("fake-message-source")
        .uid("fake-message-source-id")
        .keyBy(_.partition)

    val eventTimeProcessedStream = env
        //.addSource(consumerConfig.getConsumer)
        .addSource(new RareMessageSource)
        .setParallelism(config.sourceParallelism)
        .name("kafka-consumers")
        .uid("kafka-consumers-id")
        // tricky routine for event time
        .map(sr => {
          //(parition.next, sr._2)
          (parition.next, sr)
        })
        .name("process-per-topic")
        .uid("process-per-topic-id")
        .keyBy(_._1)
        .connect(fakeStream)
        .process(new EventTimeProcess(config.fakeMessageInterval))
        .setParallelism(config.sourceParallelism)
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
              .forBoundedOutOfOrderness[(Boolean, RareMessage)](Duration.ofMillis(config.allowedLateness))
              .withTimestampAssigner(new SerializableTimestampAssigner[(Boolean, RareMessage)] {
                override def extractTimestamp(element: (Boolean, RareMessage), recordTimestamp: Long): Long =
                  element._2.eventTime.getTime
              })
        )
        .setParallelism(config.sourceParallelism)
        .name("watermark-generation")
        .uid("watermark-generation-id")

    val mainStream = eventTimeProcessedStream
        .keyBy(_._2.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(eventProcessingInterval)))
        .reduce((v1, v2) => {
          if (v1._2.value < v2._2.value) v2
          else v1
        })
        .filter(r => r._1)
        .map(r => r._2)
        .name("main-process")
        .uid("main-prcoess-id")

    //mainStream.print()
    sink(mainStream, producerConfig)

    env.execute("Rapid window processing of rare messages on Apache Flink")
  }
}
