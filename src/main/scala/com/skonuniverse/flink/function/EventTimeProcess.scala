package com.skonuniverse.flink.function

import com.skonuniverse.flink.conifiguration.RuntimeConfig
import com.skonuniverse.flink.datatype.{FakeMessage, RareMessage}
import com.skonuniverse.flink.source.FakeMessages
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.time.Duration
import scala.reflect.classTag


/**
 * Output: (! fakeness, message)
 */
class EventTimeProcess(fakeMessageInterval: Long) extends KeyedCoProcessFunction[Int, (Int, RareMessage), FakeMessage, (Boolean, RareMessage)] {
  @transient lazy private val timestampState: ValueState[(Long, Long, Long)] = getRuntimeContext.getState(
    new ValueStateDescriptor(
      "fake-timestamp-state",
      TypeExtractor.getForClass(classTag[(Long, Long, Long)].runtimeClass).asInstanceOf[TypeInformation[(Long, Long, Long)]]
    )
  )

  //def newFakeMessage(args: AnyRef*): T = classTag[T].runtimeClass
  //    .getConstructors.head
  //    .newInstance(args: _*).asInstanceOf[T]

  override def processElement1(value: (Int, RareMessage), ctx: KeyedCoProcessFunction[Int, (Int, RareMessage), FakeMessage, (Boolean, RareMessage)]#Context, out: Collector[(Boolean, RareMessage)]): Unit = {
    if (timestampState.value() == null) timestampState.update(0L, 0L, 0L)

    val v = value._2
    val timestamps = timestampState.value()
    val newTimestamp = v.eventTime.getTime
    val timeGap = newTimestamp - timestamps._2
    val adjustedTimestamp = {
      if (Math.abs(timeGap) < 3 * fakeMessageInterval) timestamps._3 + timeGap
      else timestamps._3
    }
    val a = (newTimestamp, newTimestamp, adjustedTimestamp)
    timestampState.update(newTimestamp, newTimestamp, adjustedTimestamp)

    out.collect((true, v))
  }

  override def processElement2(value: FakeMessage, ctx: KeyedCoProcessFunction[Int, (Int, RareMessage), FakeMessage, (Boolean, RareMessage)]#Context, out: Collector[(Boolean, RareMessage)]): Unit = {
    if (timestampState.value() == null) timestampState.update(0L, 0L, 0L)

    val timestamps = timestampState.value
    val timeGap = value.timestamp - timestamps._3
    val newFakeTimestamp = timestamps._2 + timeGap
    val a = (timestamps._1, newFakeTimestamp, value.timestamp)
    this.timestampState.update(timestamps._1, newFakeTimestamp, value.timestamp)
    //val fakeMessage: T = newFakeMessage(
    //  Timestamp.from(timestamp._1.toInstant.plusMillis(timeGap)).asInstanceOf[Object],
    //  false.asInstanceOf[Object]
    //)
    val fakeMessage = RareMessage(eventTime = new Timestamp(newFakeTimestamp))

    out.collect((false, fakeMessage))
  }
}

object EventTimeProcess {
  class Partitions(paralellism: Int) extends Serializable {
    var partition = 0

    def next: Int = {
      partition = (partition + 1) % paralellism
      partition
    }
  }

  def getStream(rareMessageStream: DataStream[RareMessage], env: StreamExecutionEnvironment, config: RuntimeConfig): DataStream[(Boolean, RareMessage)] = {
    val parition = new Partitions(config.sourceParallelism)
    val fakeMessageStream = FakeMessages.getStream(env, config)

    rareMessageStream
        .map(sr => {
          //(parition.next, sr._2)
          (parition.next, sr)
        })
        .keyBy(_._1)
        .connect(fakeMessageStream)
        .process(new EventTimeProcess(config.streamIdleTimeout))
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
  }
}