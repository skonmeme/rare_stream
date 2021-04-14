package com.skonuniverse.flink.conifiguration

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import scopt.OParser

import java.util.Properties
import java.util.regex.Pattern

case class RuntimeConfig(bootstrapServer: String = "localhost:9092",
                         consumerTopics: Seq[String] = Seq("test-in"),
                         consumerTopicPattern: Pattern = null,
                         consumerProperties: Properties = new Properties(),
                         groupId: String = "test",
                         consumingOffset: String = "latest",
                         partitionDiscoveryInterval: Long = 60 * 1000L,
                         brokerList: String = "localhost:9092",
                         producerTopics: Seq[String] = Seq("test-out"),
                         producerProperties: Properties = new Properties(),
                         sematic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                         parallelism: Int = 1,
                         sourceParallelism: Int = 1,
                         sinkParallelism: Int = 1,
                         watermarkInterval: Long = 50L,
                         allowedLateness: Long = 0L,
                         restartStrategy: Properties = new Properties(),
                         checkpointInterval: Long = 600 * 1000L,
                         checkpointMode: CheckpointingMode = CheckpointingMode.EXACTLY_ONCE,
                         minPauseBetweenCheckpoints: Long = 300 * 1000L,
                         checkpointTimeout: Long = 300 * 1000L,
                         tolerableCheckpointFailureNumber: Int = 3,
                         externalizedCheckpoint: ExternalizedCheckpointCleanup = ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION,
                         maxConcurrentCheckpoints: Int = 1,
                         unalignedCheckpoints: Boolean = true,
                         alignmentTimeout: Long = 180 * 1000L,
                         stateBackend: Properties = new Properties(),
                         streamIdleTimeout: Long = 30 * 1000L)

object RuntimeConfig {
  def getConfig(args: Array[String]): RuntimeConfig = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("scopt"),
        head("scopt", "4.x"),
        opt[String]("bootstrap-server")
            .action((value, config) => config.copy(bootstrapServer = value)),
        opt[Seq[String]]("consumer-topics")
            .valueName("<topic1>,<topic2>...")
            .action((value, config) => config.copy(consumerTopics = value)),
        opt[String]("consumer-topic-pattern")
            .action((value, config) => config.copy(consumerTopicPattern = Pattern.compile(value))),
        opt[Map[String, String]]("consumerProperties")
            .valueName("key1=value1,key2=value2...")
            .action((value, config) => {
              val properties = new Properties()
              value.foreach(v => properties.put(v._1, v._2))
              config.copy(consumerProperties = properties)
            }),
        opt[String]("group-id")
            .action((value, config) => config.copy(groupId = value)),
        opt[String]("consuming-offset")
            .action((value, config) => config.copy(consumingOffset = value)),
        opt[Long]("partition-discovery-interval")
            .validate(value => {
              if (value > 0) success
              else failure("Value <partition-discovery-interval> should be greater than 0")
            })
          .action((value, config) => config.copy(partitionDiscoveryInterval = value)),
        opt[String]("broker-list")
            .action((value, config) => config.copy(brokerList = value)),
        opt[Seq[String]]("producer-topics")
            .valueName("<topic1>,<topic2>...")
            .action((value, config) => config.copy(producerTopics = value)),
        opt[Map[String, String]]("producerProperties")
            .valueName("key1=value1,key2=value2...")
            .action((value, config) => {
              val properties = new Properties()
              value.foreach(v => properties.put(v._1, v._2))
              config.copy(producerProperties = properties)
            }),
        opt[Boolean]("producer-exactly-once")
            .action((value, config) => {
              if (value) config.copy(sematic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
              else config.copy(sematic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
            }),
        opt[Int]("parallelism")
            .action((value, config) => config.copy(parallelism = value))
            .validate(value => {
              if (value > 0) success
              else failure("Value <parallelism> should be greater than 0")
            }),
        opt[Int]("source-parallelism")
            .action((value, config) => config.copy(sourceParallelism = value))
            .validate(value => {
              if (value > 0) success
              else failure("Value <sourceParallelism> should be greater than 0")
            }),
        opt[Int]("sink-parallelism")
            .action((value, config) => config.copy(sinkParallelism = value))
            .validate(value => {
              if (value > 0) success
              else failure("Value <sinkParallelism> should be greater than 0")
            }),
        opt[Long]("watermark-interval")
            .action((value, config) => config.copy(watermarkInterval = value))
            .validate(value => {
              if (value >= 0) success
              else failure("Value <watermark-interval> should be greater or equal than 0")
            }),
        opt[Long]("allowed-lateness")
            .validate(value => {
              if (value >= 0) success
              else failure("Value <allowed-lateness> should be greater or equal than 0")
            })
            .action((value, config) => config.copy(allowedLateness = value)),
        opt[Map[String, String]]("restart-strategy")
            .valueName("key1=value1,key2=value2...")
            .action((value, config) => {
              val properties = new Properties()
              value.foreach(v => properties.put(v._1, v._2))
              config.copy(restartStrategy = properties)
            }),
        opt[Long]("checkpoint-interval")
            .validate(value => {
              if (value > 0) success
              else failure("Value <checkpoint-interval> should be greater than 0")
            })
            .action((value, config) => config.copy(checkpointInterval = value)),
        opt[String]("checkpointing-mode")
            .action((value, config) => {
              if (value.startsWith("at-least")) config.copy(checkpointMode = CheckpointingMode.AT_LEAST_ONCE)
              else config.copy(checkpointMode = CheckpointingMode.EXACTLY_ONCE)
            }),
        opt[Long]("min-pause-between-checkpoints")
            .validate(value => {
              if (value > 0) success
              else failure("Value <min-pause-between-checkpoints> should be greater than 0")
            })
            .action((value, config) => config.copy(minPauseBetweenCheckpoints = value)),
        opt[Long]("checkpoint-timeout")
            .validate(value => {
              if (value > 0) success
              else failure("Value <checkpoint-timeout> should be greater than 0")
            })
            .action((value, config) => config.copy(checkpointTimeout = value)),
        opt[Int]("tolerable-checkpoint-failure-number")
            .validate(value => {
              if (value > 0) success
              else failure("Value <tolerable-checkpoint-failure-number> should be greater than 0")
            })
            .action((value, config) => config.copy(tolerableCheckpointFailureNumber = value)),
        opt[String]("externalized-checkpoint")
            .action((value, config) => {
              if (value.startsWith("delete")) config.copy(externalizedCheckpoint = ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
              else config.copy(externalizedCheckpoint = ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
            }),
        opt[Int]("max-concurrent-checkpoints")
            .validate(value => {
              if (value > 0) success
              else failure("Value <max-concurrent-checkpoints> should be greater than 0")
            })
            .action((value, config) => config.copy(maxConcurrentCheckpoints = value)),
        opt[Boolean]("unaligned-checkpoints")
            .action((value, config) => config.copy(unalignedCheckpoints = value)),
        opt[Long]("alignment-timeout")
            .validate(value => {
              if (value >= 0) success
              else failure("Value <alignment-timeout> should be greater or equal than 0")
            })
            .action((value, config) => config.copy(alignmentTimeout = value)),
        opt[Map[String, String]]("state-backend")
            .valueName("key1=value1,key2=value2...")
            .action((value, config) => {
              val properties = new Properties()
              value.foreach(v => properties.put(v._1, v._2))
              config.copy(stateBackend = properties)
            }),
        opt[Long]("fake-message-interval")
            .validate(value => {
              if (value >= 0) success
              else failure("Value <fake-message-interval> should be greater or equal than 0")
            })
            .action((value, config) => config.copy(streamIdleTimeout = value))
      )
    }

    OParser.parse(parser, args, RuntimeConfig()) match {
      case Some(config) =>
        config
      case _ =>
        new RuntimeConfig()
      // arguments are bad, error message will have been displayed
    }
  }
}
