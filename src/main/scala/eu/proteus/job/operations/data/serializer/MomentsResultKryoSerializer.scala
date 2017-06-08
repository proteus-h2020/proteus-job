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
import eu.proteus.job.operations.data.results.{MomentsResult, MomentsResult1D, MomentsResult2D}


class MomentsResultKryoSerializer
  extends Serializer[MomentsResult]
  with Serializable {

  override def read(kryo: Kryo, input: Input, t: Class[MomentsResult]): MomentsResult = {

    val magicNumber = input.readInt()
    assert(magicNumber == MAGIC_NUMBER)

    val coilId = input.readInt()
    val varId = input.readInt()

    val mtype = input.readByte()
    val x = input.readDouble()

    val y = if (mtype == MOMENTS_RESULT_2D) {
      input.readDouble()
    } else {
      0.0
    }

    val mean = input.readDouble()
    val variance = input.readDouble()
    val counter = input.readDouble()

    if (mtype == MOMENTS_RESULT_2D) {
      MomentsResult2D(coilId, varId, x, y, mean, variance, counter)
    } else if (mtype == MOMENTS_RESULT_1D) {
      MomentsResult1D(coilId, varId, x, mean, variance, counter)
    } else {
      throw new UnsupportedOperationException
    }

  }

  override def write(kryo: Kryo, output: Output, obj: MomentsResult): Unit = {

    output.writeInt(MAGIC_NUMBER)

    output.writeInt(obj.coilId)
    output.writeInt(obj.varId)

    obj match {
      case m1d: MomentsResult1D => {
        output.writeByte(MOMENTS_RESULT_1D)
        output.writeDouble(m1d.x)
      }
      case m2d: MomentsResult2D => {
        output.writeByte(MOMENTS_RESULT_2D)
        output.writeDouble(m2d.x)
        output.writeDouble(m2d.y)
      }
    }

    output.writeDouble(obj.mean)
    output.writeDouble(obj.variance)
    output.writeDouble(obj.counter)

  }
}

