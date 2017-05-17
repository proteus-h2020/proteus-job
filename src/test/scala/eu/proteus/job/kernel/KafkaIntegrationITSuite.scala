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

import java.lang.reflect.Field
import java.util.{Properties, UUID}

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.serializer.CoilMeasurementKryoSerializer
import kafka.common.NotLeaderForPartitionException
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducerBase, KafkaTestBase, KafkaTestEnvironment}
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchemaWrapper, TypeInformationSerializationSchema}
import org.apache.flink.test.util.SuccessException
import org.apache.flink.testutils.junit.RetryOnException
import org.junit.{AfterClass, BeforeClass, Test}
import org.scalatest.junit.JUnitSuiteLike

import scala.concurrent.duration.FiniteDuration

@Test(timeout = 60000)
@RetryOnException (times = 2, exception = classOf[NotLeaderForPartitionException] )
class KafkaIntegrationITSuite
  extends KafkaTestBase
  with JUnitSuiteLike {

  import KafkaIntegrationITSuite._

  @Test
  def runSimpleIntegrationTest(): Unit = {

    val topic = "kafkaProducerConsumerTopic_" + UUID.randomUUID.toString

    val parallelism = 1
    val elementsPerPartition = 100
    val totalElements = parallelism * elementsPerPartition

    JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout))

    kafkaServer.createTestTopic(topic, parallelism, 1)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.enableCheckpointing(500)
    env.setRestartStrategy(RestartStrategies.noRestart) // fail immediately

    env.getConfig.disableSysoutLogging

    env.getConfig.registerTypeWithKryoSerializer(classOf[CoilMeasurement], classOf[CoilMeasurementKryoSerializer])
    env.getConfig.registerTypeWithKryoSerializer(classOf[SensorMeasurement2D], classOf[CoilMeasurementKryoSerializer])
    env.getConfig.registerTypeWithKryoSerializer(classOf[SensorMeasurement1D], classOf[CoilMeasurementKryoSerializer])

    val typeInfo = TypeInformation.of(classOf[CoilMeasurement])
    val schema = new TypeInformationSerializationSchema[CoilMeasurement](typeInfo, env.getConfig)

    // ----------- add producer dataflow ----------

    val s1 = SensorMeasurement1D(0, 12.0, 0 to 0, FlinkDenseVector(1.0))
    val s2 = SensorMeasurement2D(0, 12.0, 1.0, 0 to 0, FlinkDenseVector(1.0))
    val stream = env.addSource((ctx: SourceContext[CoilMeasurement]) => {
      ctx.collect(s1)
      ctx.collect(s2)
    })

    val producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings)
    producerProperties.setProperty("retries", "3")
    producerProperties.putAll(secureProps)

    kafkaServer.produceIntoKafka(
      stream.javaStream,
      topic,
      new KeyedSerializationSchemaWrapper[CoilMeasurement](schema),
      producerProperties,
      null)

    // ----------- add consumer dataflow ----------

    val props = new Properties
    props.putAll(standardProps)
    props.putAll(secureProps)

    val source = kafkaServer.getConsumer(topic, schema, props)
    val consuming = env.addSource(source).setParallelism(parallelism)

    consuming.addSink(new SinkFunction[CoilMeasurement]() {
      var e = 0
      override def invoke(in: CoilMeasurement): Unit = {
        in match {
          case s1d: SensorMeasurement1D =>
            e += 1
            Predef.assert(s1d == s1)
          case s2d: SensorMeasurement2D =>
            e += 1
            Predef.assert(s2d == s2)
        }
        if (e == 2) {
          throw new SuccessException
        }
      }
    }).setParallelism(1)

    try
      KafkaTestBase.tryExecutePropagateExceptions(env.getJavaEnv, "runSimpleIntegrationTest")
    catch {
      case e@(_: ProgramInvocationException | _: JobExecutionException) =>
        // look for NotLeaderForPartitionException
        var cause = e.getCause
        // search for nested SuccessExceptions
        var depth = 0
        while ( {
          cause != null && {
            depth += 1;
            depth - 1
          } < 20
        }) {
            if (cause.isInstanceOf[NotLeaderForPartitionException])
              throw cause.asInstanceOf[Exception]
            cause = cause.getCause
        }
        throw e
    }

    KafkaTestBase.deleteTestTopic(topic)

  }


}

object KafkaIntegrationITSuite {

  @BeforeClass
  def prepare = KafkaTestBase.prepare()

  @AfterClass
  def shutdown = KafkaTestBase.shutDownServices()

  private def extractField(field: String): Field = {
    val f = classOf[KafkaTestBase].getDeclaredField(field)
    f.setAccessible(true)
    f
  }

  private val __timeout = extractField("timeout")

  def timeout = __timeout.get(null).asInstanceOf[FiniteDuration]

  private val __flink = extractField("flink")

  def flink = __flink.get(null).asInstanceOf[LocalFlinkMiniCluster]

  private val __kafka = extractField("kafkaServer")

  def kafkaServer = __kafka.get(null).asInstanceOf[KafkaTestEnvironment]

  private val __brokerConnectionStrings = extractField("brokerConnectionStrings")

  def brokerConnectionStrings = __brokerConnectionStrings.get(null).asInstanceOf[String]

  private val __secureProps = extractField("secureProps")

  def secureProps = __secureProps.get(null).asInstanceOf[Properties]

  private val __standardProps = extractField("standardProps")

  def standardProps = __standardProps.get(null).asInstanceOf[Properties]

}