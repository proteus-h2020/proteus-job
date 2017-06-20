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

    val r1 = new SAXResult(0, 1, 2, "classId", 0.0d)
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
