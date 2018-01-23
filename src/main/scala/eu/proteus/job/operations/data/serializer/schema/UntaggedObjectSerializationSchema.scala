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

package eu.proteus.job.operations.data.serializer.schema

import java.io.{EOFException, IOException}

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.Output
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.java.typeutils.runtime.{DataInputViewStream, DataOutputViewStream, NoFetchingInput}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

import scala.reflect.ClassTag

class UntaggedObjectSerializationSchema[T: TypeInformation : ClassTag](cfg: ExecutionConfig)
    extends DeserializationSchema[T]
    with SerializationSchema[T] {

  @transient
  private [schema] var dis: DataInputDeserializer = _

  @transient
  private [schema] var dos: DataOutputSerializer = _

  @transient
  private [schema] var noFetchingInput: NoFetchingInput = _

  @transient
  private [schema] var output: Output = _

  private [schema] val typeInfo = implicitly[TypeInformation[T]]

  private [schema] val serializer = typeInfo.createSerializer(cfg)

  override def isEndOfStream(nextElement: T): Boolean = false

  override def deserialize(message: Array[Byte]): T = {
    if (dis != null) {
      dis.setBuffer(message, 0, message.length)
    } else {
      dis = new DataInputDeserializer(message, 0, message.length)
      noFetchingInput = new NoFetchingInput(new DataInputViewStream(dis))
    }
    try {
      serializer match {
        case k: KryoSerializer[T] => {
          val kryo = k.getKryo
          try {
            kryo.readObject(noFetchingInput, typeInfo.getTypeClass)
          } catch {
            case ke: KryoException => {
              ke.getCause match {
                case cause: EOFException => throw cause
                case _ => throw ke
              }
            }
          }
        }
        case _ => {
          throw new UnsupportedOperationException
        }
      }
    } catch {
      case e: IOException => {
        throw new RuntimeException("Unable to deserialize message", e)
      }
    }
  }

  override def serialize(element: T): Array[Byte] = {
    if (dos == null) {
      dos = new DataOutputSerializer(16)
      output = new Output(new DataOutputViewStream(dos))
    }

    try {
      serializer match {
        case k: KryoSerializer[T] => {
          val kryo = k.getKryo
          // Sanity check: Make sure that the output is cleared/has been flushed by the last call
          // otherwise data might be written multiple times in case of a previous EOFException
          if (output.position != 0) {
            throw new IllegalStateException("The Kryo Output still contains data from a previous " +
              "serialize call. It has to be flushed or cleared at the end of the serialize call.")
          }
          try {
            kryo.writeObject(output, element)
            output.flush()
          } catch {
            case ke: KryoException => {
              ke.getCause match {
                case cause: EOFException => throw cause
                case _ => throw ke
              }
            }
          }
        }
        case _ => {
          throw new UnsupportedOperationException
        }
      }
    } catch {
      case e: IOException => {
        throw new RuntimeException("Unable to serialize record", e)
      }
    }
    var ret = dos.getByteArray
    if (ret.length != dos.length) {
      val n = new Array[Byte](dos.length)
      System.arraycopy(ret, 0, n, 0, dos.length)
      ret = n
    }
    dos.clear()
    ret
  }

  override def getProducedType: TypeInformation[T] = typeInfo
}
