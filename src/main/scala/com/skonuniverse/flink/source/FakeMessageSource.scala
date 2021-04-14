package com.skonuniverse.flink.source

import com.skonuniverse.flink.conifiguration.RuntimeConfig
import com.skonuniverse.flink.datatype.FakeMessage
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

class FakeMessageSource(streamIdleTimeout: Long, parallelism: Int) extends SourceFunction[FakeMessage] {
  override def run(ctx: SourceFunction.SourceContext[FakeMessage]): Unit = {
    var timestamp = 0L
    var partition = 0
    while (true) {
      Thread.sleep(streamIdleTimeout)
      timestamp += streamIdleTimeout
      partition = (partition + 1) % parallelism
      ctx.collect(FakeMessage(partition, timestamp))
    }
  }

  override def cancel(): Unit = {}
}

object FakeMessages {
  def getStream(env: StreamExecutionEnvironment, config: RuntimeConfig): KeyedStream[FakeMessage, Int] = {
    env
        .addSource(new FakeMessageSource(config.streamIdleTimeout, config.sourceParallelism))
        .setParallelism(config.sourceParallelism)
        .name("fake-message-source")
        .uid("fake-message-source-id")
        .keyBy(_.partition)
  }
}