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
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import eu.proteus.job.operations.data.serializer.SAXResultKryoSerializer
import org.scalatest.FunSuite
import org.scalatest.Matchers

/**
 * Serialization test suite.
 */
class SAXResultKryoSerializerUTSuite extends FunSuite with Matchers{

  test("SAX serialization"){

    val kryo = new Kryo()
    val serializer = new SAXResultKryoSerializer
    kryo.register(classOf[SAXResult], serializer)

    val r1 = new SAXResult(0, "var1", 1.0d, 2.0d, "classId", 0.0d)
    val out = new Output(1024)
    val in = new Input(out.getBuffer)
    kryo.writeObject(out, r1)

    val serialized = kryo.readObject(in, classOf[SAXResult])
    assert(r1.coilId == serialized.coilId, "Coil Id should match")
    assert(r1.x1 == serialized.x1, "X1 should match")
    assert(r1.x2 == serialized.x2, "X2 should match")
    assert(r1.classId == serialized.classId, "Class Id should match")
    assert(r1.similarity == serialized.similarity, "Class Id should match")

  }

}
