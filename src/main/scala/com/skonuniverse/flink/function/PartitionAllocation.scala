package com.skonuniverse.flink.function

import com.skonuniverse.flink.datatype.RareMessage
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class PartitionAllocation extends ProcessFunction[RareMessage, (Int, RareMessage)] {
  var tasks: Int = 0
  var partition: Int = 0

  override def processElement(value: RareMessage, ctx: ProcessFunction[RareMessage, (Int, RareMessage)]#Context, out: Collector[(Int, RareMessage)]): Unit = {
    partition = (partition + 1) % tasks
    out.collect((partition, value))
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    partition = getRuntimeContext.getIndexOfThisSubtask
    tasks = getRuntimeContext.getNumberOfParallelSubtasks
  }
}
