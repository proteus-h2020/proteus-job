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
import eu.proteus.job.operations.serializer.MomentsResultKryoSerializer
import org.junit.runner.RunWith
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class MomentsResultKryoSerializerUTSuite
  extends WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  "test kryo serialization" in {

    val kryo = new Kryo()
    val ser = new MomentsResultKryoSerializer
    kryo.register(classOf[MomentsResult], ser)
    kryo.register(classOf[MomentsResult1D], ser)
    kryo.register(classOf[MomentsResult2D], ser)

    val m1D = MomentsResult1D(1, 1, 13.5, 1.0, Double.NaN, 1.0)
    val m2D = MomentsResult2D(4, 2, 53.5, 0.546, 2.05, 0.45767, 12.0)

    val out = new Output(1024)
    val in = new Input(out.getBuffer)

    kryo.writeObject(out, m1D)
    kryo.writeObject(out, m2D)

    val m1 = kryo.readObject(in, classOf[MomentsResult])

    an [TestFailedException] should be thrownBy {
      // NaN is not equal to NaN
      m1 shouldEqual m1D
    }

    m1.variance.isNaN shouldEqual m1D.variance.isNaN

    val m2 = kryo.readObject(in, classOf[MomentsResult])
    m2 shouldEqual m2D


  }

}