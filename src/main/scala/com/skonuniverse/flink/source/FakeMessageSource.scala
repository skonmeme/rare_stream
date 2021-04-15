package com.skonuniverse.flink.source

import com.skonuniverse.flink.conifiguration.RuntimeConfig
import com.skonuniverse.flink.datatype.FakeMessage
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

class FakeMessageSource(streamIdleTimeout: Long) extends RichParallelSourceFunction[FakeMessage] {
  lazy val index: Int = getRuntimeContext.getIndexOfThisSubtask
  lazy val tasks: Int = getRuntimeContext.getNumberOfParallelSubtasks

  override def run(ctx: SourceFunction.SourceContext[FakeMessage]): Unit = {
    var timestamp = 0L
    var partition = index
    while (true) {
      Thread.sleep(streamIdleTimeout)
      timestamp += streamIdleTimeout
      partition = (partition + 1) % tasks
      ctx.collect(FakeMessage(partition, timestamp))
    }
  }

  override def cancel(): Unit = {}
}

object FakeMessages {
  def getStream(env: StreamExecutionEnvironment, config: RuntimeConfig): KeyedStream[FakeMessage, Int] = {
    env
        .addSource(new FakeMessageSource(config.streamIdleTimeout))
        .setParallelism(config.sourceParallelism)
        .name("fake-message-source")
        .uid("fake-message-source-id")
        .keyBy(_.partition)
  }
}