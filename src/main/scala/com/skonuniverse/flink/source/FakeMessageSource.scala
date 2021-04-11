package com.skonuniverse.flink.source

import com.skonuniverse.flink.datatype.RareMessage
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.sql.Timestamp
import java.time.Instant
import scala.reflect.classTag

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
