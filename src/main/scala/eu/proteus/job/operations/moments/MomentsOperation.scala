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

import java.beans.Transient

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.results.{MomentsResult, MomentsResult1D, MomentsResult2D}
import eu.proteus.solma.moments.MomentsEstimator
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object MomentsOperation {

  def runSimpleMomentsAnalytics(
      stream: DataStream[CoilMeasurement],
      featuresCount: Int
  ): DataStream[MomentsResult] = {

    implicit val typeInfo = TypeInformation.of(classOf[(CoilMeasurement, Int)])

    var kstream: KeyedStream[(CoilMeasurement, Int), Int] = null

    val momentsEstimator = MomentsEstimator()
      .setFeaturesCount(1)
      .enableAggregation(false)
      .setPartitioning((in) => {

        kstream = in
          .asInstanceOf[DataStream[(CoilMeasurement)]]
          .map(
            (coilMeasurement: CoilMeasurement) => {
              val sensorId = coilMeasurement.slice.head
              (coilMeasurement, coilMeasurement.coilId * featuresCount + sensorId)
            }
          )
          .keyBy(x => x._2)

        kstream.map(
          (t) => {
            val (coilMeasurement, pid) = t
            val clone = coilMeasurement match {
              case s1d: SensorMeasurement1D => s1d.copy(slice = 0 to 0)
              case s2d: SensorMeasurement2D => s2d.copy(slice = 0 to 0)
            }
            (clone.asInstanceOf[CoilMeasurement], pid)
            }
        )
        .keyBy(x => x._2)
        .asInstanceOf[KeyedStream[(Any, Int), Int]]
      })

    val moments = momentsEstimator.transform(stream)

    kstream
      .connect(moments.keyBy(x => x._1))
      .flatMap(new RichCoFlatMapFunction[(CoilMeasurement, Int), (Int, MomentsEstimator.Moments), MomentsResult]() {

        @Transient
        private var map: MapState[Int, mutable.Queue[(Option[Double], Option[Double])]] = _

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          val descriptor = new MapStateDescriptor[Int, mutable.Queue[(Option[Double], Option[Double])]](
            "coords",
            TypeInformation.of(classOf[Int]),
            TypeInformation.of(classOf[mutable.Queue[(Option[Double], Option[Double])]])
          )
          map = getRuntimeContext.getMapState(descriptor)
        }

        override def flatMap1(value: (CoilMeasurement, Int), out: Collector[MomentsResult]): Unit = {
          val key = value._1.coilId * featuresCount + value._1.slice.head
          val v = value._1 match {
            case s1d: SensorMeasurement1D => (Some(s1d.x), None)
            case s2d: SensorMeasurement2D => (Some(s2d.x), Some(s2d.y))
          }
          val queue = if (map.contains(key)) {
            map.get(key)
          } else {
            val q = mutable.Queue[(Option[Double], Option[Double])]()
            map.put(key, q)
            q
          }
          queue += v
        }

        override def flatMap2(in: (Int, MomentsEstimator.Moments), out: Collector[MomentsResult]): Unit = {
          val (pid: Int, metrics: MomentsEstimator.Moments) = in
          val cid = pid % featuresCount
          val sid = pid / featuresCount
          val q = map.get(pid)
          if (q.isEmpty) {
            throw new RuntimeException("this queue is not supposed to be empty!")
          }
          val (ox, oy) = q.dequeue
          val result = oy match {
            case Some(y) => MomentsResult2D(cid, sid, metrics.mean(0), ox.get, y, metrics.variance(0), metrics.counter(0))
            case None => MomentsResult1D(cid, sid, metrics.mean(0), ox.get, metrics.variance(0), metrics.counter(0))
          }
          out.collect(result)
        }

    })
    .name("coil-xy-join")
    .uid("coil-xy-join")


  }

}
