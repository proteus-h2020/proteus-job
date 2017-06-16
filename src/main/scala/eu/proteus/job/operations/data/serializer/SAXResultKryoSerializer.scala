package eu.proteus.job.operations.data.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import eu.proteus.job.operations.data.results.SAXResult

/**
 * Kryo serializer for SAX results.
 * {{{class SAXResult(coilId: Int, x1: Long, x2: Long, classId: String, similarity: Double)}}}
 */
class SAXResultKryoSerializer
  extends Serializer[SAXResult]
  with Serializable {

  override def write(kryo: Kryo, output: Output, r: SAXResult): Unit = {
    output.writeInt(MAGIC_NUMBER)
    output.writeInt(r.coilId)
    output.writeLong(r.x1)
    output.writeLong(r.x2)
    output.writeString(r.classId)
    output.writeDouble(r.similarity)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[SAXResult]): SAXResult = {
    val magicNumber = input.readInt()
    assert(magicNumber == MAGIC_NUMBER)

    val coilId : Int = input.readInt()
    val x1: Long = input.readLong()
    val x2 : Long = input.readLong()
    val classId : String = input.readString()
    val similarity : Double = input.readDouble()

    new SAXResult(coilId, x1, x2, classId, similarity)
  }
}
