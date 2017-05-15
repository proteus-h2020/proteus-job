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

package eu.proteus.job.operations.data.model

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import eu.proteus.job.operations.serializer.CoilMeasurementKryoSerializer
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class CoilMeasurementKryoSerializerUTSuite
  extends WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  "test kryo serialization" in {

    val kryo = new Kryo()
    val ser = new CoilMeasurementKryoSerializer
    kryo.register(classOf[CoilMeasurement], ser)
    kryo.register(classOf[SensorMeasurement1D], ser)
    kryo.register(classOf[SensorMeasurement2D], ser)

    val measurement1D = SensorMeasurement1D(0, 12.0, 0 to 0, FlinkDenseVector(1.0))
    val measurement2D = SensorMeasurement2D(0, 12.0, 1.0, 0 to 0, FlinkDenseVector(1.0))

    val out = new Output(1024)
    val in = new Input(out.getBuffer)

    kryo.writeObject(out, measurement1D)
    kryo.writeObject(out, measurement2D)

    val s1 = kryo.readObject(in, classOf[CoilMeasurement])
    val s2 = kryo.readObject(in, classOf[CoilMeasurement])

    s1 shouldEqual measurement1D
    s2 shouldEqual measurement2D

  }

}
