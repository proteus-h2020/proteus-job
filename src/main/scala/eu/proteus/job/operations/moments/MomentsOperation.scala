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

package eu.proteus.job.operations.moments

import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.results.MomentsResult
import eu.proteus.solma.moments.MomentsEstimator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

object MomentsOperation {

  def runSimpleMomentsAnalytics(
      stream: DataStream[CoilMeasurement],
      featuresCount: Int
 ): DataStream[MomentsResult] = {

    implicit val typeInfo = TypeInformation.of(classOf[(CoilMeasurement, Int)])

    val momentsEstimator = MomentsEstimator()
      .setFeaturesCount(1)
      .enableAggregation(false)
      .setPartitioning((in) => {
        in
          .asInstanceOf[DataStream[(CoilMeasurement)]]
          .map(
          (coilMeasurement: CoilMeasurement) => {
            val sensorId = coilMeasurement.slice.head
            coilMeasurement.slice = 0 to 0
            (coilMeasurement, coilMeasurement.coilId * featuresCount + sensorId)
            }
          )
          .keyBy(x => x._2)
          .asInstanceOf[KeyedStream[(Any, Int), Int]]
      })

    val moments = momentsEstimator.transform(stream)

    moments.flatMap((t, out) => {
      val (pid: Int, metrics: MomentsEstimator.Moments) = t
      val variance = metrics.variance(0)
      if (!java.lang.Double.isNaN(variance)) {
        val cid = pid % featuresCount
        val sid = pid / featuresCount
        out.collect(MomentsResult(cid, sid, metrics.mean(0), variance, metrics.counter(0)))
      }
    })

  }

}
