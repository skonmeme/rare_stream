package com.skonuniverse.flink.source

import com.skonuniverse.flink.datatype.FakeMessage
import org.apache.flink.streaming.api.functions.source.SourceFunction

/*
class FakeMessageSource[T <: RareMessage](generationInterval: Long) extends SourceFunction[T] {
  def newFakeMessage(args: AnyRef*): T = classTag[T].runtimeClass
      .getConstructors.head
      .newInstance(args: _*).asInstanceOf[T]

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    var timestamp = 0L
    while (true) {
      Thread.sleep(generationInterval)
      timestamp += generationInterval
      val fakeMessage: T = newFakeMessage(
        Timestamp.from(Instant.ofEpochMilli(timestamp)).asInstanceOf[Object],
        false.asInstanceOf[Object]
      )
      ctx.collectWithTimestamp(fakeMessage, Instant.now().toEpochMilli)
    }
  }

  override def cancel(): Unit = _
}
*/

class FakeMessageSource(generattionInterval: Long, parallelism: Int) extends SourceFunction[FakeMessage] {
  override def run(ctx: SourceFunction.SourceContext[FakeMessage]): Unit = {
    var timestamp = 0L
    var partition = 0
    while (true) {
      Thread.sleep(generattionInterval)
      timestamp += generattionInterval
      partition = (partition + 1) % parallelism
      ctx.collect(FakeMessage(partition, timestamp))
    }
  }

  override def cancel(): Unit = {}
}