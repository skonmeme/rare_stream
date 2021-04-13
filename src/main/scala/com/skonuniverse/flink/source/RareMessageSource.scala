package com.skonuniverse.flink.source

import com.skonuniverse.flink.datatype.RareMessage
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.sql.Timestamp
import scala.util.Random

class RareMessageSource extends SourceFunction[RareMessage] {
  private val interval = 30 * 1000L

  override def run(ctx: SourceFunction.SourceContext[RareMessage]): Unit = {
    val random = Random
    while (true) {
      val timestamp = new Timestamp(System.currentTimeMillis())
      ctx.collect(RareMessage(eventTime = timestamp, key = random.nextInt(10), value = random.nextDouble()))
      Thread.sleep(interval)
    }
  }

  override def cancel(): Unit = {}
}