package eu.proteus.job.operations.data

package object serializer {

  private [serializer] val MAGIC_NUMBER = 0x00687691 // PROTEUS EU id

  private [serializer] val COIL_MEASUREMENT_1D = 0x00
  private [serializer] val COIL_MEASUREMENT_2D = 0x01

  private [serializer] val MOMENTS_RESULT_1D = 0x00
  private [serializer] val MOMENTS_RESULT_2D = 0x01


}
