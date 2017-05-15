package eu.proteus.job.operations.data.model

import eu.proteus.solma.events.StreamEvent
import org.apache.flink.ml.math.Vector

trait CoilMeasurement extends StreamEvent {
  def coilId: Int
}

case class SensorMeasurement1D(
    coilId: Int,
    x: Double,
    slice: IndexedSeq[Int],
    data: Vector
  ) extends CoilMeasurement

case class SensorMeasurement2D(
    coilId: Int,
    x: Double,
    y: Double,
    slice: IndexedSeq[Int],
    data: Vector
  ) extends CoilMeasurement
