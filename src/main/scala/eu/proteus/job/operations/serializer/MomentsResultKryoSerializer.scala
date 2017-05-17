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

package eu.proteus.job.operations.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import eu.proteus.job.operations.data.results.MomentsResult


class MomentsResultKryoSerializer extends Serializer[MomentsResult] {
  override def read(kryo: Kryo, input: Input, t: Class[MomentsResult]): MomentsResult = {
    throw new UnsupportedOperationException
  }

  override def write(kryo: Kryo, output: Output, obj: MomentsResult): Unit = {
    output.writeString(obj.toJson)
  }
}
