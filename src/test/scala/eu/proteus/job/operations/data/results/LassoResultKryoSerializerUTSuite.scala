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

package eu.proteus.job.operations.data.results

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import eu.proteus.job.operations.data.serializer.LassoResultKryoSerializer
import org.scalatest.{FunSuite, Matchers}

/**
 * Serialization test suite.
 */
class LassoResultKryoSerializerUTSuite extends FunSuite with Matchers{

  test("Lasso serialization"){

    val kryo = new Kryo()
    val serializer = new LassoResultKryoSerializer
    kryo.register(classOf[LassoResult], serializer)

    val r1 = new LassoResult(0, 1, 1.0d, 2.0d)
    val out = new Output(1024)
    val in = new Input(out.getBuffer)
    kryo.writeObject(out, r1)

    val serialized = kryo.readObject(in, classOf[LassoResult])
    assert(r1.coilId == serialized.coilId, "Coil Id should match")
    assert(r1.varId == serialized.varId, "Var Id should match")
    assert(r1.x == serialized.x, "X should match")
    assert(r1.label == serialized.label, "Label should match")
  }

}
