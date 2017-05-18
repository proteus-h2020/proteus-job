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

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

trait MomentsResult extends Serializable {

  def coilId: Int
  def varId: Int
  def mean: Double
  def variance: Double
  def counter: Double

  def toJson: String = {
    implicit val formats = DefaultFormats
    write(this)
  }

}

case class MomentsResult1D(
  coilId: Int,
  varId: Int,
  x: Double,
  mean: Double,
  variance: Double,
  counter: Double) extends MomentsResult


case class MomentsResult2D(
  coilId: Int,
  varId: Int,
  x: Double,
  y: Double,
  mean: Double,
  variance: Double,
  counter: Double) extends MomentsResult


