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

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import eu.proteus.job.operations.data.results.LassoResult

/**
 * Kryo serializer for Lasso results.
 * {{{
 *  class LassoResult(
 *    val coilId: Long,
 *    var varId: Int,
 *    val x: Double,
 *    val label: Double)
 * }}}
 *
 * Notice that the variableName is serialized as Integer. The serializer expects a varName
 * in the form of CXXX where X is a number.
 */
class LassoResultKryoSerializer
  extends Serializer[LassoResult]
  with Serializable {

  override def write(kryo: Kryo, output: Output, r: LassoResult): Unit = {
    output.writeInt(MAGIC_NUMBER)
    output.writeLong(r.coilId)
    output.writeInt(r.varId)
    output.writeDouble(r.x)
    output.writeDouble(r.label)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[LassoResult]): LassoResult = {
    val magicNumber = input.readInt()
    assert(magicNumber == MAGIC_NUMBER)

    val coilId: Long = input.readLong()
    val varId: Int = input.readInt()
    val x: Double = input.readDouble()
    val label: Double = input.readDouble()

    new LassoResult(coilId, varId, x, label)
  }
}
