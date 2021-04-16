package com.skonuniverse.flink.source

import com.skonuniverse.flink.conifiguration.RuntimeConfig
import com.skonuniverse.flink.datatype.FakeMessage
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

class FakeMessageSource(streamIdleTimeout: Long) extends ParallelSourceFunction[FakeMessage] {
  override def run(ctx: SourceFunction.SourceContext[FakeMessage]): Unit = {
    var timestamp = 0L
    while (true) {
      Thread.sleep(streamIdleTimeout)
      timestamp += streamIdleTimeout
      ctx.collect(FakeMessage(timestamp))
    }
  }

  override def cancel(): Unit = {}
}

object FakeMessages {
  def getStream(env: StreamExecutionEnvironment, config: RuntimeConfig): DataStream[FakeMessage] = {
    env
        .addSource(new FakeMessageSource(config.streamIdleTimeout))
        .setParallelism(config.sourceParallelism)
        .name("fake-message-source")
        .uid("fake-message-source-id")
  }
}