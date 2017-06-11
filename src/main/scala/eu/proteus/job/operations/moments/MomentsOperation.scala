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
      featuresCount: Long
  ): DataStream[MomentsResult] = {

    implicit val typeInfo = createTypeInformation[(CoilMeasurement, Long)]

    var kstream: KeyedStream[(CoilMeasurement, Long), Long] = null

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
              val vkey = coilMeasurement.coilId.toLong * featuresCount + sensorId.toLong
              (coilMeasurement, vkey)
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
        .asInstanceOf[KeyedStream[(Any, Long), Long]]
      })

    val moments = momentsEstimator.transform(stream)

    kstream
      .connect(moments.keyBy(x => x._1))
      .flatMap(new CoilsMomentsJoin(featuresCount))
    .uid("coil-xy-join")
  }

  class CoilsMomentsJoin(featuresCount: Long)
    extends RichCoFlatMapFunction[(CoilMeasurement, Long), (Long, MomentsEstimator.Moments), MomentsResult]() {

    @transient
    private var coordsMap: MapState[Long, mutable.Queue[(Option[Double], Option[Double])]] = _

    @transient
    private var orphanMoments: MapState[Long, mutable.Queue[MomentsEstimator.Moments]] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      val cfg = getRuntimeContext.getExecutionConfig
      val keyType = createTypeInformation[Long]
      val valueType1 = createTypeInformation[mutable.Queue[(Option[Double], Option[Double])]]
      val valueType2 = createTypeInformation[mutable.Queue[MomentsEstimator.Moments]]

      val coordsDescriptor = new MapStateDescriptor[Long, mutable.Queue[(Option[Double], Option[Double])]](
        "coords",
        keyType.createSerializer(cfg),
        valueType1.createSerializer(cfg)
      )
      val momentsDescriptor = new MapStateDescriptor[Long, mutable.Queue[MomentsEstimator.Moments]](
        "moments",
        keyType.createSerializer(cfg),
        valueType2.createSerializer(cfg)
      )
      coordsMap = getRuntimeContext.getMapState(coordsDescriptor)
      orphanMoments = getRuntimeContext.getMapState(momentsDescriptor)
    }

    private def joinAndOutput(metrics: MomentsEstimator.Moments, cid: Int, sid: Int,
        ox: Option[Double], oy: Option[Double], out: Collector[MomentsResult]
    ): Unit = {
      val result = oy match {
        case Some(y) => MomentsResult2D(cid, sid, ox.get, y,
          metrics.mean(0), metrics.variance(0), metrics.counter(0))
        case None => MomentsResult1D(cid, sid, ox.get,
          metrics.mean(0), metrics.variance(0), metrics.counter(0))
      }
      out.collect(result)
    }

    override def flatMap1(value: (CoilMeasurement, Long), out: Collector[MomentsResult]): Unit = {
      val cid = value._1.coilId
      val sid = value._1.slice.head
      val pid = cid.toLong * featuresCount + sid.toLong
      val coords = value._1 match {
        case s1d: SensorMeasurement1D => (Some(s1d.x), None)
        case s2d: SensorMeasurement2D => (Some(s2d.x), Some(s2d.y))
      }
      if (orphanMoments.contains(pid)) {
        val momentsQueue = orphanMoments.get(pid)
        if (momentsQueue.nonEmpty) {
          val m = momentsQueue.dequeue
          if (!m.variance(0).isNaN) {
            joinAndOutput(m, cid, sid, coords._1, coords._2, out)
          }
          // needed in case of rocksdb backend
          // todo introduce better queue data type
          orphanMoments.put(pid, momentsQueue)
        }
      } else {
        val coordsQueue = if (coordsMap.contains(pid)) {
          coordsMap.get(pid)
        } else {
          mutable.Queue[(Option[Double], Option[Double])]()
        }
        coordsQueue += coords
        // needed in case of rocksdb backend
        // todo introduce better queue data type
        coordsMap.put(pid, coordsQueue)
      }

    }

    override def flatMap2(in: (Long, MomentsEstimator.Moments), out: Collector[MomentsResult]): Unit = {
      val (pid: Long, metrics: MomentsEstimator.Moments) = in
      val cid = pid / featuresCount
      val sid = pid % featuresCount
      val coordsQueue = coordsMap.get(pid)
      if (coordsQueue == null || coordsQueue.isEmpty) {
        val momentsQueue = if (orphanMoments.contains(pid)) {
          orphanMoments.get(pid)
        } else {
          mutable.Queue[MomentsEstimator.Moments]()
        }
        momentsQueue += metrics
        // needed in case of rocksdb backend
        // todo introduce better queue data type
        orphanMoments.put(pid, momentsQueue)
      } else {
        val (ox, oy) = coordsQueue.dequeue
        if (!metrics.variance(0).isNaN) {
          joinAndOutput(metrics, cid.toInt, sid.toInt, ox, oy, out)
        }
        // needed in case of rocksdb backend
        // todo introduce better queue data type
        coordsMap.put(pid, coordsQueue)
      }

    }

  }

}
