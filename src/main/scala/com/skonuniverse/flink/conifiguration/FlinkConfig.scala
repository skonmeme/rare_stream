package com.skonuniverse.flink.conifiguration

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

import java.util.Properties
import java.util.concurrent.TimeUnit


case class FlinkConfig(parallelism: Int = 1,
                       sourceParallelism: Int = 1,
                       sinkParallelism: Int = 1,
                       watermarkInterval: Long = 50L,
                       restartStrategy: RestartStrategyConfiguration = RestartStrategies.noRestart,
                       checkpointInterval: Long = 0L,
                       checkpointingMode: CheckpointingMode = CheckpointingMode.EXACTLY_ONCE,
                       minPauseBetweenCheckpoints: Long = 300 * 1000L,
                       checkpointTimeout: Long = 300 * 1000L,
                       tolerableCheckpointFailureNumber: Int = 3,
                       externalizedCheckpoint: ExternalizedCheckpointCleanup = ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION,
                       maxConcurrentCheckpoints: Int = 1,
                       unalignedCheckpoints: Boolean = true,
                       alignmentTimeout: Long = 180 * 1000L,
                       stateBackend: StateBackend = new MemoryStateBackend(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE, false))

object FlinkConfig {
  def getRestartStrategy(strategyProperties: Properties): RestartStrategyConfiguration = {
    strategyProperties.getProperty("strategy", "") match {
      case "failure-rate" =>
        RestartStrategies.failureRateRestart(
          strategyProperties.getProperty("failures", "3").toInt,
          Time.of(strategyProperties.getProperty("interval", "600000").toLong, TimeUnit.MILLISECONDS),
          Time.of(strategyProperties.getProperty("delay", "300000").toLong, TimeUnit.MILLISECONDS)
        )
      case "fixed-delay" =>
        RestartStrategies.fixedDelayRestart(
          strategyProperties.getProperty("attempts", "3").toInt,
          Time.of(strategyProperties.getProperty("delay", "300000").toLong, TimeUnit.MILLISECONDS)
        )
      case _ => RestartStrategies.noRestart()
    }
  }

  def getStateBackend(backendProperties: Properties): StateBackend = {
    backendProperties.getProperty("backend", "memory") match {
      case "fs" | "filesystem" =>
        new FsStateBackend(
          backendProperties.getProperty("url", "file:///var/tmp/flink/checkpoints/"),
          false
        )
      case "rocksdb" =>
        new RocksDBStateBackend(
          backendProperties.getProperty("url", "file:///var/tmp/flink/checkpoints"),
          backendProperties.getProperty("incremental", "true").toBoolean
        )
      case _ =>
        new MemoryStateBackend(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE, false)
    }
  }

  def getConfig(config: RuntimeConfig): FlinkConfig = {
    new FlinkConfig(
      parallelism = config.parallelism,
      sourceParallelism = config.sourceParallelism,
      sinkParallelism = config.sinkParallelism,
      watermarkInterval = config.watermarkInterval,
      restartStrategy = getRestartStrategy(config.restartStrategy),
      checkpointInterval = config.checkpointInterval,
      checkpointingMode = config.checkpointMode,
      minPauseBetweenCheckpoints = config.minPauseBetweenCheckpoints,
      checkpointTimeout = config.checkpointTimeout,
      tolerableCheckpointFailureNumber = config.tolerableCheckpointFailureNumber,
      externalizedCheckpoint = config.externalizedCheckpoint,
      maxConcurrentCheckpoints = config.maxConcurrentCheckpoints,
      unalignedCheckpoints = config.unalignedCheckpoints,
      alignmentTimeout = config.alignmentTimeout,
      stateBackend = getStateBackend(config.stateBackend)
    )
  }
}
