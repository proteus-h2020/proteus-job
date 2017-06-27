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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import eu.proteus.job.operations.data.results.SAXResult

/**
 * Kryo serializer for SAX results.
 * {{{
 *  class SAXResult(
 *    coilId: Int,
 *    varName: String,
 *    x1: Double,
 *    x2: Double,
 *    classId: String,
 *    similarity: Double)
 * }}}
 *
 * Notice that the variableName is serialized as Integer. The serializer expects a varName
 * in the form of CXXX where X is a number.
 */
class SAXResultKryoSerializer
  extends Serializer[SAXResult]
  with Serializable {

  override def write(kryo: Kryo, output: Output, r: SAXResult): Unit = {
    output.writeInt(MAGIC_NUMBER)
    output.writeInt(r.coilId)
    output.writeInt(r.varName.replace("C", "").toInt)
    output.writeDouble(r.x1)
    output.writeDouble(r.x2)
    output.writeString(r.classId)
    output.writeDouble(r.similarity)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[SAXResult]): SAXResult = {
    val magicNumber = input.readInt()
    assert(magicNumber == MAGIC_NUMBER)

    val coilId : Int = input.readInt()
    val varName : String = input.readInt().toString
    val x1: Double = input.readDouble()
    val x2 : Double = input.readDouble()
    val classId : String = input.readString()
    val similarity : Double = input.readDouble()

    new SAXResult(coilId, varName, x1, x2, classId, similarity)
  }
}
