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

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.results.{MomentsResult, MomentsResult1D, MomentsResult2D}
import eu.proteus.solma.moments.MomentsEstimator
import grizzled.slf4j.Logger
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object MomentsOperation {

  private [moments] val LOG = Logger(getClass)

  def runSimpleMomentsAnalytics(
      stream: DataStream[CoilMeasurement],
      featuresCount: Int
  ): DataStream[String] = {

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
              if (LOG.isDebugEnabled) {
                LOG.debug("Reading sensor id %d for coil %d".format(sensorId, coilMeasurement.coilId))
              }
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

        @transient
        private var coordsMap: MapState[Int, mutable.Queue[(Option[Double], Option[Double])]] = _

        @transient
        private var earlyMoments: MapState[Int, mutable.Queue[MomentsEstimator.Moments]] = _

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          val coordsDescriptor = new MapStateDescriptor[Int, mutable.Queue[(Option[Double], Option[Double])]](
            "coords",
            TypeInformation.of(classOf[Int]),
            TypeInformation.of(classOf[mutable.Queue[(Option[Double], Option[Double])]])
          )
          val momentsDescriptor = new MapStateDescriptor[Int, mutable.Queue[MomentsEstimator.Moments]](
            "moments",
            TypeInformation.of(classOf[Int]),
            TypeInformation.of(classOf[mutable.Queue[MomentsEstimator.Moments]])
          )
          coordsMap = getRuntimeContext.getMapState(coordsDescriptor)
          earlyMoments = getRuntimeContext.getMapState(momentsDescriptor)
        }

        private def joinAndOutput(metrics: MomentsEstimator.Moments, cid: Int, sid: Int,
            ox: Option[Double], oy: Option[Double], out: Collector[MomentsResult]
        ): Unit = {
          val result = oy match {
            case Some(y) => MomentsResult2D(cid, sid, ox.get, y, metrics.mean(0), metrics.variance(0), metrics.counter(0))
            case None => MomentsResult1D(cid, sid, ox.get, metrics.mean(0), metrics.variance(0), metrics.counter(0))
          }
          out.collect(result)
        }

        override def flatMap1(value: (CoilMeasurement, Int), out: Collector[MomentsResult]): Unit = {
          val cid = value._1.coilId
          val sid = value._1.slice.head
          val pid = cid * featuresCount + sid
          val coords = value._1 match {
            case s1d: SensorMeasurement1D => (Some(s1d.x), None)
            case s2d: SensorMeasurement2D => (Some(s2d.x), Some(s2d.y))
          }
          if (earlyMoments.contains(pid)) {
            val momentsQueue = earlyMoments.get(pid)
            if (momentsQueue.nonEmpty) {
              val m = momentsQueue.dequeue
              if (!m.variance(0).isNaN) {
                joinAndOutput(m, cid, sid, coords._1, coords._2, out)
              }
            }
          } else {
            val coordsQueue = if (coordsMap.contains(pid)) {
              coordsMap.get(pid)
            } else {
              val q = mutable.Queue[(Option[Double], Option[Double])]()
              coordsMap.put(pid, q)
              q
            }
            coordsQueue += coords
          }

        }

        override def flatMap2(in: (Int, MomentsEstimator.Moments), out: Collector[MomentsResult]): Unit = {
          val (pid: Int, metrics: MomentsEstimator.Moments) = in
          val cid = pid % featuresCount
          val sid = pid / featuresCount
          val coordsQueue = coordsMap.get(pid)
          if (coordsQueue == null || coordsQueue.isEmpty) {
            val momentsQueue = if (earlyMoments.contains(pid)) {
              earlyMoments.get(pid)
            } else {
              val q = mutable.Queue[MomentsEstimator.Moments]()
              earlyMoments.put(pid, q)
              q
            }
            momentsQueue += metrics
          } else {
            val (ox, oy) = coordsQueue.dequeue
            if (!metrics.variance(0).isNaN) {
              joinAndOutput(metrics, cid, sid, ox, oy, out)
            }
          }

        }

    })
    .uid("coil-xy-join")
//    .map(_.toJson)
    .map(result => {
      val ret = result.toJson
      if (LOG.isDebugEnabled) {
        LOG.debug("Sinking to kafka: " + ret)
      }
      ret
    })
  }

}
