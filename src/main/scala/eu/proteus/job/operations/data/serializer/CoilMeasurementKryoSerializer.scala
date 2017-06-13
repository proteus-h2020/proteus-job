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

package eu.proteus.job.operations.data.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}


class CoilMeasurementKryoSerializer
  extends Serializer[CoilMeasurement]
  with Serializable {

  override def read(kryo: Kryo, input: Input, t: Class[CoilMeasurement]): CoilMeasurement = {

    val magicNumber = input.readInt()
    assert(magicNumber == MAGIC_NUMBER, "unknown magic number")
    val measurementType = input.readByte()
    assert(measurementType == COIL_MEASUREMENT_1D || measurementType == COIL_MEASUREMENT_2D,
        "unknown coil type")
    val is2D = (measurementType & 0x01) == 1
    val coilId = input.readInt()

    val x = input.readDouble()
    val y = if (is2D) {
      input.readDouble()
    } else {
      0.0
    }

    val variableId = input.readInt()
    val value = input.readDouble()

    if (is2D) {
      SensorMeasurement2D(coilId, x, y, variableId to variableId, FlinkDenseVector(value))
    } else {
      SensorMeasurement1D(coilId, x, variableId to variableId, FlinkDenseVector(value))
    }
  }

  override def write(kryo: Kryo, output: Output, obj: CoilMeasurement): Unit = {

    output.writeInt(MAGIC_NUMBER)
    val sm = obj match {
      case s1d: SensorMeasurement1D =>
        output.writeByte(COIL_MEASUREMENT_1D)
        output.writeInt(obj.coilId)
        output.writeDouble(s1d.x)
        s1d
      case s2d: SensorMeasurement2D =>
        output.writeByte(COIL_MEASUREMENT_2D)
        output.writeInt(obj.coilId)
        output.writeDouble(s2d.x)
        output.writeDouble(s2d.y)
        s2d
    }
    output.writeInt(sm.slice.head)
    output.writeDouble(sm.data(0))
  }
}
