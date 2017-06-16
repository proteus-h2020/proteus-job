package eu.proteus.job.operations.data.results

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

/**
 * Result emitted by the SAX algorithm
 *
 * @param coilId Coil identifier.
 * @param x1 The starting X value.
 * @param x2 The ending X value.
 * @param classId The class identifier.
 * @param similarity The similarity value with the class.
 */
class SAXResult(
  val coilId: Int,
  val x1: Long,
  val x2: Long,
  val classId: String,
  val similarity: Double
) extends Serializable{

  /**
   * Transform the class into a JSON string.
   * @return The JSON representation.
   */
  def toJson: String = {
    implicit val formats = DefaultFormats
    write(this)
  }

}
