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

package eu.proteus.job.operations.lasso

import java.lang.reflect.Field
import java.util.{Properties, UUID}

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.results.LassoResult
import eu.proteus.job.operations.data.serializer.{CoilMeasurementKryoSerializer, LassoResultKryoSerializer}
import eu.proteus.job.operations.data.serializer.schema.UntaggedObjectSerializationSchema
import grizzled.slf4j.Logger
import kafka.common.NotLeaderForPartitionException
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducerBase, KafkaTestBase, KafkaTestEnvironment}
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.test.util.SuccessException
import org.apache.flink.testutils.junit.RetryOnException
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

import scala.concurrent.duration.FiniteDuration

@Test(timeout = 60000)
@RetryOnException (times = 2, exception = classOf[NotLeaderForPartitionException] )
class LassoITSuite
  extends KafkaTestBase
    with JUnitSuiteLike {

  import LassoITSuite._

  private [lasso] val LOG = Logger(getClass)

  // scalastyle:off

  @Test
  def runLassoIntegrationWithKafka(): Unit = {

    val topicMeasurement = "kafkaProducerConsumerTopicMeasurement_" + UUID.randomUUID.toString
    val topicFlatness = "kafkaProducerConsumerTopicFlatness_" + UUID.randomUUID.toString

    val sourceAndSinkparallelism = 1
    val lassoParallelism = 1

    JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout))

    kafkaServer.createTestTopic(topicMeasurement, sourceAndSinkparallelism, 1)
    kafkaServer.createTestTopic(topicFlatness, sourceAndSinkparallelism, 1)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(sourceAndSinkparallelism)
    //env.enableCheckpointing(500)
    env.setRestartStrategy(RestartStrategies.noRestart) // fail immediately

    env.getConfig.disableSysoutLogging

    env.getConfig.registerTypeWithKryoSerializer(classOf[CoilMeasurement], classOf[CoilMeasurementKryoSerializer])
    env.getConfig.registerTypeWithKryoSerializer(classOf[SensorMeasurement2D], classOf[CoilMeasurementKryoSerializer])
    env.getConfig.registerTypeWithKryoSerializer(classOf[SensorMeasurement1D], classOf[CoilMeasurementKryoSerializer])
    env.getConfig.registerTypeWithKryoSerializer(classOf[LassoResult], classOf[LassoResultKryoSerializer])

    implicit val typeInfo = createTypeInformation[CoilMeasurement]
    val schema = new UntaggedObjectSerializationSchema[CoilMeasurement](env.getConfig)

    // ----------- add producer dataflow ----------
    val streamMeasurement = env.addSource((ctx: SourceContext[CoilMeasurement]) => {
      for (m <- dataMeasurement) {
        ctx.collect(m)
      }
    })

    val streamFlatness = env.addSource((ctx: SourceContext[CoilMeasurement]) => {
      for (m <- dataFlatness) {
        ctx.collect(m)
      }
    })

    val producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings)
    producerProperties.setProperty("retries", "3")
    producerProperties.putAll(secureProps)

    kafkaServer.produceIntoKafka(
      streamFlatness.javaStream,
      topicFlatness,
      new KeyedSerializationSchemaWrapper[CoilMeasurement](schema),
      producerProperties,
      null)

    kafkaServer.produceIntoKafka(
      streamMeasurement.javaStream,
      topicMeasurement,
      new KeyedSerializationSchemaWrapper[CoilMeasurement](schema),
      producerProperties,
      null)

    // ----------- add consumer dataflow ----------

    val props = new Properties
    props.putAll(standardProps)
    props.putAll(secureProps)

    val sourceMeasurement = kafkaServer.getConsumer(topicMeasurement, schema, props)
    val consumingMeasurement = env.addSource(sourceMeasurement).setParallelism(sourceAndSinkparallelism)
    val sourceFlatness = kafkaServer.getConsumer(topicFlatness, schema, props)
    val consumingFlatness = env.addSource(sourceFlatness).setParallelism(sourceAndSinkparallelism)

    env.setParallelism(lassoParallelism)

    val workerParallelism = 4
    val psParallelism = 1
    val pullLimit = 10
    val featureCount = 76
    val rangePartitioning = true
    val allowedFlatnessLateness = 10
    val allowedRealtimeLateness = 25
    val iterationWaitTime: Long = 20000
    val varName = "C0028"

    val operation = new LassoOperation(varName, workerParallelism, psParallelism, pullLimit, featureCount,
      rangePartitioning, allowedFlatnessLateness, allowedRealtimeLateness, iterationWaitTime)

    val result = operation.runLasso(consumingMeasurement, consumingFlatness)

    result.addSink(new RichSinkFunction[LassoResult]() {
      var e = 0
      override def invoke(in: LassoResult): Unit = {
        e += 1
        if (e >= LassoITSuite.dataMeasurement.length) {
          throw new SuccessException
        }
      }

      override def close(): Unit = {
      }
    }).setParallelism(sourceAndSinkparallelism)

    try {
      LOG.info(env.getExecutionPlan)
      KafkaTestBase.tryExecutePropagateExceptions(env.getJavaEnv, "runLassoIT")
    } catch {
      case e@(_: ProgramInvocationException | _: JobExecutionException) =>
        // look for NotLeaderForPartitionException
        var cause = e.getCause
        // search for nested SuccessExceptions
        var depth = 0
        while ( {
          cause != null && {
            depth += 1
            depth - 1
          } < 20
        }) {
          if (cause.isInstanceOf[NotLeaderForPartitionException]) {
            throw cause.asInstanceOf[Exception]
          }
          cause = cause.getCause
        }
        throw e
    }

    KafkaTestBase.deleteTestTopic(topicMeasurement)
    KafkaTestBase.deleteTestTopic(topicFlatness)

  }

  // scalastyle:on

}

object LassoITSuite {


  val dataMeasurement = Seq(
    SensorMeasurement1D(0, 5.0, 0 to 0, FlinkDenseVector(1.0)),
    SensorMeasurement1D(0, 6.0, 1 to 1, FlinkDenseVector(1.2)),
    SensorMeasurement1D(1, 4.0, 0 to 0, FlinkDenseVector(1.5)),
    SensorMeasurement1D(1, 8.0, 1 to 1, FlinkDenseVector(1.1)),
    SensorMeasurement1D(2, 2.0, 2 to 2, FlinkDenseVector(1.0)),
    SensorMeasurement1D(2, 3.0, 0 to 0, FlinkDenseVector(1.2)),
    SensorMeasurement1D(2, 12.0, 1 to 1, FlinkDenseVector(1.0))
  )

  val dataFlatness = Seq(
    SensorMeasurement1D(0, 12.0, 28 to 28, FlinkDenseVector(1.0)),
    SensorMeasurement1D(0, 13.0, 28 to 28, FlinkDenseVector(1.2)),
    SensorMeasurement1D(1, 14.0, 28 to 28, FlinkDenseVector(1.5)),
    SensorMeasurement1D(1, 17.0, 28 to 28, FlinkDenseVector(1.1)),
    SensorMeasurement1D(2, 18.0, 28 to 28, FlinkDenseVector(1.0)),
    SensorMeasurement1D(2, 19.0, 28 to 28, FlinkDenseVector(1.2))
  )

  private def extractField(field: String): Field = {
    val f = classOf[KafkaTestBase].getDeclaredField(field)
    f.setAccessible(true)
    f
  }

  private val __timeout = extractField("timeout")

  def timeout: FiniteDuration = __timeout.get(null).asInstanceOf[FiniteDuration]

  private val __flink = extractField("flink")

  def flink: LocalFlinkMiniCluster = __flink.get(null).asInstanceOf[LocalFlinkMiniCluster]

  private val __kafka = extractField("kafkaServer")

  def kafkaServer: KafkaTestEnvironment = __kafka.get(null).asInstanceOf[KafkaTestEnvironment]

  private val __brokerConnectionStrings = extractField("brokerConnectionStrings")

  def brokerConnectionStrings: String = __brokerConnectionStrings.get(null).asInstanceOf[String]

  private val __secureProps = extractField("secureProps")

  def secureProps: Properties = __secureProps.get(null).asInstanceOf[Properties]

  private val __standardProps = extractField("standardProps")

  def standardProps: Properties = __standardProps.get(null).asInstanceOf[Properties]

}
