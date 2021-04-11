package com.skonuniverse.flink.function

import com.skonuniverse.flink.datatype.{DefaultRareMessage, RareMessage}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import java.sql.Timestamp
import java.time.Instant
import scala.reflect.classTag


class EventTimeProcess[T <: RareMessage] extends CoProcessFunction[T, T, T] {
  var timestampState: ValueState[(Timestamp, Timestamp)]

  def newFakeMessage(args: AnyRef*): T = classTag[T].runtimeClass
      .getConstructors.head
      .newInstance(args: _*).asInstanceOf[T]

  override def processElement1(value: T, ctx: CoProcessFunction[T, T, T]#Context, out: Collector[T]): Unit = {
    val timestamp = this.timestampState.value()
    if (value.fakeness) {
      val timeGap = value.eventTime.getTime - timestamp._2.getTime
      this.timestampState.update(timestamp._1, value.eventTime)
      val fakeMessage: T = newFakeMessage(
        Timestamp.from(value.eventTime.toInstant.plusMillis(timeGap)).asInstanceOf[Object],
        false.asInstanceOf[Object]
      )
      out.collect(fakeMessage)
    } else {
      this.timestampState.update((value.eventTime, timestamp._2))
      out.collect(value)
    }
  }

  override def processElement2(value: T, ctx: CoProcessFunction[T, T, T]#Context, out: Collector[T]): Unit = {
    val timestamp = this.timestampState.value()
    if (value.fakeness) {
      val timeGap = value.eventTime.getTime - timestamp._2.getTime
      this.timestampState.update(timestamp._1, value.eventTime)
      val fakeMessage: T = newFakeMessage(
        Timestamp.from(value.eventTime.toInstant.plusMillis(timeGap)).asInstanceOf[Object],
        false.asInstanceOf[Object]
      )
      out.collect(fakeMessage)
    } else {
      this.timestampState.update((value.eventTime, timestamp._2))
    }
  }

  override def open(parameters: Configuration): Unit = {
    this.timestampState = {
      getRuntimeContext.getState(new ValueStateDescriptor("fake-timestamp-state", createTypeInformation[(Timestamp, Timestamp)]))
    }
    this.timestampState.update((new Timestamp(0L), new Timestamp(0L)))
  }
}
