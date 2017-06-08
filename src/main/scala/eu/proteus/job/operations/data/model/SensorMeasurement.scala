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

package eu.proteus.job.operations.data.model

import eu.proteus.solma.events.StreamEvent
import org.apache.flink.ml.math.Vector

trait CoilMeasurement extends StreamEvent {
  def coilId: Int
}

case class SensorMeasurement1D(
    coilId: Int,
    x: Double,
    var slice: IndexedSeq[Int],
    data: Vector
  ) extends CoilMeasurement

case class SensorMeasurement2D(
    coilId: Int,
    x: Double,
    y: Double,
    var slice: IndexedSeq[Int],
    data: Vector
  ) extends CoilMeasurement
