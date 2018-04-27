/*
 * Copyright (C) 2017 The Proteus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.job.kernel

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}
import java.util.Properties

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.results._
import eu.proteus.job.operations.data.serializer.{CoilMeasurementKryoSerializer, LassoResultKryoSerializer, MomentsResultKryoSerializer, SAXResultKryoSerializer}
import eu.proteus.job.operations.data.serializer.schema.UntaggedObjectSerializationSchema
import eu.proteus.job.operations.lasso.LassoOperation
import eu.proteus.job.operations.moments.MomentsOperation
import eu.proteus.job.operations.sax.SAXOperation
import eu.proteus.solma
import grizzled.slf4j.Logger
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.collection.mutable


object ProteusJob {

  /**
   * Flag to launch the moments operation. Use this flag for debugging purposes.
   */
  private val LinkMoments : Boolean = true

  /**
   * Flag to launch the SAX operation. Use this flag for debugging purposes.
   */
  private val LinkSAX : Boolean = true

  /**
    * Flag to launch the Lasso operation. Use this flag for debugging purposes.
    */
  private val LinkLasso : Boolean = true

  private val SAXJobs : JMap[String, SAXOperation] = new JHashMap[String, SAXOperation]()
  private val LassoJobs : JMap[String, LassoOperation] = new JHashMap[String, LassoOperation]()

  private [kernel] val LOG = Logger(getClass)
  private [kernel] val ONE_MEGABYTE = 1024 * 1024
  private [kernel] val ONE_MINUTE_IN_MS = 60 * 1000


  // kafka config
  private [kernel] var kafkaBootstrapServer = "localhost:2181"
  private [kernel] var realtimeDataKafkaTopic = "proteus-realtime"
  private [kernel] var processedRealtimeDataKafkaTopic = "proteus-processed-realtime"
  private [kernel] var flatnessDataKafkaTopic = "proteus-flatness"
  private [kernel] var jobStackBackendType = "memory"

  // flink config
  private [kernel] var flinkCheckpointsDir = ""
  private [kernel] var memoryBackendMBSize = 20
  private [kernel] var flinkCheckpointsInterval = 10
  private [kernel] var enableExaclyOnceGuarantees = false
  private [kernel] var iterativeJob = true


  def loadBaseKafkaProperties = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaBootstrapServer)
    properties
  }

  def loadConsumerKafkaProperties = {
    val properties = loadBaseKafkaProperties
    properties
  }


  def configureFlinkEnv(env: StreamExecutionEnvironment) = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    if (jobStackBackendType == "rocksdb") {
      val stateBackend = new RocksDBStateBackend(flinkCheckpointsDir, false)
//      stateBackend.setPredefinedOptions(PredefinedOptions.DEFAULT)
      env.setStateBackend(stateBackend)
    } else if (jobStackBackendType == "memory") {
      val stateBackend = new MemoryStateBackend(memoryBackendMBSize * ONE_MEGABYTE, true)
      env.setStateBackend(stateBackend)
    } else {
      throw new UnsupportedOperationException
    }

    val timeout = flinkCheckpointsInterval * ONE_MINUTE_IN_MS

    val checkpointingMode = if (enableExaclyOnceGuarantees) {
      CheckpointingMode.EXACTLY_ONCE
    } else {
      CheckpointingMode.AT_LEAST_ONCE
    }

    /*
      NOTE: Checkpointing iterative streaming dataflows in not properly supported at
    * the moment. If the "force" parameter is set to true, the system will execute the
    * job nonetheless.
    * */
    val force = iterativeJob

    env.enableCheckpointing(timeout, checkpointingMode, force)

    val cfg = env.getConfig

    // register types
    cfg.registerKryoType(classOf[CoilMeasurement])
    cfg.registerKryoType(classOf[SensorMeasurement2D])
    cfg.registerKryoType(classOf[SensorMeasurement1D])
    cfg.registerKryoType(classOf[MomentsResult])
    cfg.registerKryoType(classOf[MomentsResult1D])
    cfg.registerKryoType(classOf[MomentsResult2D])
    cfg.registerKryoType(classOf[solma.moments.MomentsEstimator.Moments])
    cfg.registerKryoType(classOf[SAXResult])
    cfg.registerKryoType(classOf[LassoResult])

    // register serializers
    env.addDefaultKryoSerializer(classOf[CoilMeasurement], classOf[CoilMeasurementKryoSerializer])
    env.addDefaultKryoSerializer(classOf[SensorMeasurement2D], classOf[CoilMeasurementKryoSerializer])
    env.addDefaultKryoSerializer(classOf[SensorMeasurement1D], classOf[CoilMeasurementKryoSerializer])
    env.addDefaultKryoSerializer(classOf[MomentsResult], classOf[MomentsResultKryoSerializer])
    env.addDefaultKryoSerializer(classOf[MomentsResult1D], classOf[MomentsResultKryoSerializer])
    env.addDefaultKryoSerializer(classOf[MomentsResult2D], classOf[MomentsResultKryoSerializer])
    env.addDefaultKryoSerializer(classOf[SAXResult], classOf[SAXResultKryoSerializer])
    env.addDefaultKryoSerializer(classOf[LassoResult], classOf[LassoResultKryoSerializer])

    env.addDefaultKryoSerializer(classOf[mutable.Queue[_]],
      classOf[com.twitter.chill.TraversableSerializer[_, mutable.Queue[_]]])
  }


  /**
   * Start the PROTEUS Job in Flink. The job contains several operations including moments and SAX.
   * @param parameters The parameters.
   */
  def startProteusJob(parameters: ParameterTool) : Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    configureFlinkEnv(env)

    implicit val inputTypeInfo = createTypeInformation[CoilMeasurement]
    val inputSchema = new UntaggedObjectSerializationSchema[CoilMeasurement](env.getConfig)

    val realtimeSource: DataStream[CoilMeasurement] = env.addSource(new FlinkKafkaConsumer010[CoilMeasurement](
        realtimeDataKafkaTopic,
        inputSchema,
        loadConsumerKafkaProperties))

    val flatnessSource: DataStream[CoilMeasurement] = env.addSource(new FlinkKafkaConsumer010[CoilMeasurement](
        flatnessDataKafkaTopic,
        inputSchema,
        loadConsumerKafkaProperties))

    LOG.info("Processed real-time output topic: processed-real-time")
    val realtimeSinkSchema = new UntaggedObjectSerializationSchema[CoilMeasurement](env.getConfig)
    val producerCfg = FlinkKafkaProducer010.writeToKafkaWithTimestamps(realtimeSource.javaStream,
      processedRealtimeDataKafkaTopic, realtimeSinkSchema, loadBaseKafkaProperties)
    producerCfg.setLogFailuresOnly(false)
    producerCfg.setFlushOnCheckpoint(true)

    // simple moments
    if(ProteusJob.LinkMoments){
      LOG.info("Moments output topic: simple-moments")
      val moments = MomentsOperation.runSimpleMomentsAnalytics(realtimeSource, 54)
      implicit val momentsTypeInfo = createTypeInformation[MomentsResult]
      val momentsSinkSchema = new UntaggedObjectSerializationSchema[MomentsResult](env.getConfig)
      val producerCfg = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
        moments.javaStream,
        "simple-moments",
        momentsSinkSchema,
        loadBaseKafkaProperties)
      producerCfg.setLogFailuresOnly(false)
      producerCfg.setFlushOnCheckpoint(true)
    }

    if(ProteusJob.LinkSAX){
      val variables = parameters.getRequired("sax-variable").split(",")
      val saxSinkSchema = new UntaggedObjectSerializationSchema[SAXResult](env.getConfig)
      variables.foreach(varName => {
        val saxJob = new SAXOperation(
          parameters.getRequired("sax-model-storage-path"),
          varName
        )
        val saxJobResult = saxJob.runSAX(realtimeSource)
        this.SAXJobs.put(varName, saxJob)
        val outputProducer = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
          saxJobResult.javaStream,
          "sax-results",
          saxSinkSchema,
          loadBaseKafkaProperties)
        outputProducer.setLogFailuresOnly(false)
        outputProducer.setFlushOnCheckpoint(true)
      })
    }

    if(ProteusJob.LinkLasso){
      val variables = parameters.getRequired("lasso-variable").split(",")
      val lassoSinkSchema = new UntaggedObjectSerializationSchema[LassoResult](env.getConfig)
      val workerParallelism = parameters.getRequired("lasso-workers").toInt
      val psParallelism = parameters.getRequired("lasso-ps").toInt
      val pullLimit = parameters.getRequired("lasso-pull-limit").toInt
      val featureCount = parameters.getRequired("lasso-features").toInt
      val rangePartitioning = parameters.getRequired("lasso-range-partitioning").toBoolean
      val allowedFlatnessLateness = parameters.getRequired("lasso-flatness-allowed-lateness").toInt
      val allowedRealtimeLateness = parameters.getRequired("lasso-realtime-allowed-lateness").toInt
      val iterationWaitTime: Long = parameters.getRequired("lasso-iteration-wait-time").toLong

      variables.foreach{
        varName =>
          val lassoJob = new LassoOperation(varName, workerParallelism, psParallelism, pullLimit, featureCount,
            rangePartitioning, allowedFlatnessLateness, allowedRealtimeLateness, iterationWaitTime)
          val lassoJobResult = lassoJob.runLasso(realtimeSource, flatnessSource)
          LassoJobs.put(varName, lassoJob)
          val outputProducer = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
            lassoJobResult.javaStream,
            "lasso-results",
            lassoSinkSchema,
            loadBaseKafkaProperties)
          outputProducer.setLogFailuresOnly(false)
          outputProducer.setFlushOnCheckpoint(true)
      }

    }

    env.execute("The Proteus Job")
  }

  def printUsage = {
    System.out.println("The Flink Kafka Job")
    System.out.println("Parameters:")
    System.out.println("--bootstrap-server\tKafka Bootstrap Server")
    System.out.println("--state-backend\tFlink State Backend [memory|rocksdb]")
    System.out.println("--state-backend-mbsize\tFlink Memory State Backend size in MB (default: 20)")
    System.out.println("--flink-checkpoints-interval\tFlink Checkpoints Interval in mins (default: 10)")
    System.out.println("--flink-exactly-once\tThis enables Flink Exactly-once" +
      " processing guarantee (disabled by default)")
    System.out.println("--flink-checkpoints-dir\tAn HDFS dir that " +
      "stores rocksdb checkpoints, e.g., hdfs://namenode:9000/flink-checkpoints/")
    System.out.println("--sax-model-storage-path\tThe path where the trained dictionary will be stored")
    System.out.println("--sax-variable\tThe variable to be analyzed by SAX. Supports lists with comma")
    System.out.println("--lasso-variable\tThe variable to be analyzed by Lasso. Supports lists with comma")
    System.out.println("--lasso-workers\tNumber of workers")
    System.out.println("--lasso-ps\tParameter server parallelism")
    System.out.println("--lasso-pull-limit\tPull limit")
    System.out.println("--lasso-features\tNumber of features")
    System.out.println("--lasso-range-partitioning\tRange partitioning (boolean)")
    System.out.println("--lasso-flatness-allowed-lateness\tAllowed flatness lateness")
    System.out.println("--lasso-realtime-allowed-lateness\tAllowed realtime lateness")
    System.out.println("--lasso-iteration-wait-time\tIteration wait time")
  }

  def main(args: Array[String]): Unit = {

    var parameters: ParameterTool = null
    try {
      parameters = ParameterTool.fromArgs(args)
      kafkaBootstrapServer = parameters.getRequired("bootstrap-server")

      jobStackBackendType = parameters.get("state-backend")
      assert(jobStackBackendType == "memory" || jobStackBackendType == "rocksdb")

      if (jobStackBackendType == "rocksdb") {
        flinkCheckpointsDir = parameters.getRequired("flink-checkpoints-dir")
      }

      if (jobStackBackendType == "memory") {
        memoryBackendMBSize = parameters.getInt("state-backend-mbsize", 20)
      }

      flinkCheckpointsInterval = parameters.getInt("flink-checkpoints-interval", 10)

      enableExaclyOnceGuarantees = parameters.getBoolean("flink-exactly-once", false)

    } catch {
      case t: Throwable =>
        LOG.error("Error parsing the command line!")
        printUsage
        System.exit(-1)
    }

    try {
      startProteusJob(parameters)
    } catch {
      case t: Throwable =>
        LOG.error("Failed to run the Proteus Flink Job!")
        LOG.error(t.getMessage, t)
    }

  }

}
