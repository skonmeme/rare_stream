package com.skonuniverse.flink.specification

import com.skonuniverse.flink.conifiguration.FlinkConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink {
  def setEnvironment(env: StreamExecutionEnvironment, config: FlinkConfig): Unit = {
    val maxParallelism = Math.max(Math.max(config.parallelism, config.sourceParallelism), config.sinkParallelism)

    env.setMaxParallelism(maxParallelism + maxParallelism / 2 + 1)
    env.setParallelism(config.parallelism)
    env.setRestartStrategy(config.restartStrategy)
    env.getConfig.setAutoWatermarkInterval(config.watermarkInterval)
    if (config.checkpointInterval > 0L) {
      env.enableCheckpointing(config.checkpointInterval)
      env.getCheckpointConfig.setCheckpointingMode(config.checkpointingMode)
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.minPauseBetweenCheckpoints)
      env.getCheckpointConfig.setCheckpointTimeout(config.checkpointTimeout)
      env.getCheckpointConfig.setTolerableCheckpointFailureNumber(config.tolerableCheckpointFailureNumber)
      env.getCheckpointConfig.enableExternalizedCheckpoints(config.externalizedCheckpoint)
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(config.maxConcurrentCheckpoints)
      env.getCheckpointConfig.enableUnalignedCheckpoints(config.unalignedCheckpoints)
      env.getCheckpointConfig.setAlignmentTimeout(config.alignmentTimeout)
      env.setStateBackend(config.stateBackend)
    }
  }
}
