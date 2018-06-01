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

package eu.proteus.job.operations.lasso

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D, SensorMeasurement2D}
import eu.proteus.job.operations.data.results.LassoResult
import eu.proteus.solma.events.{StreamEventLabel, StreamEventWithPos}
import eu.proteus.solma.lasso.Lasso.LassoParam
import eu.proteus.solma.lasso.{LassoDFStreamTransformOperation, LassoDelayedFeedbacks}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import breeze.linalg.{Vector => BreezeVector}
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class SensorMeasurement(pos: (Long, Double),
                             var slice: IndexedSeq[Int],
                             data: Vector) extends StreamEventWithPos[(Long, Double)]

case class FlatnessMeasurement(poses: List[Double],
                               label: Long,
                               labels: DenseVector,
                               var slice: IndexedSeq[Int],
                               data: Vector) extends StreamEventLabel[Long, Double]

class AggregateFlatnessValuesWindowFunction extends ProcessWindowFunction[CoilMeasurement, LassoStreamEvent, Int,
  TimeWindow] {

  override def process(key: Int, context: Context, in: Iterable[CoilMeasurement],
                       out: Collector[LassoStreamEvent]): Unit = {
    val iter = in.toList
    val poses: List[Double] = iter.map{
      x => x match {
          case s1d: SensorMeasurement1D => s1d.x
          case s2d: SensorMeasurement2D => s2d.x
        }
    }
    val labels: DenseVector = new DenseVector(iter.map(x => x.data.head._2).toArray)
    val flat: FlatnessMeasurement =
      FlatnessMeasurement(poses, iter.head.coilId, labels, in.head.slice, in.head.data)
    val ev: StreamEventLabel[Long, Double] = flat
    out.collect(Right(ev))
  }

}


class AggregateMeasurementValuesWindowFunction(val featureCount: Int) extends ProcessWindowFunction[CoilMeasurement,
  LassoStreamEvent, (Int, Double), TimeWindow] {
  override def process(key:(Int, Double), context: Context, in: Iterable[CoilMeasurement],
                       out: Collector[LassoStreamEvent]): Unit = {


    val iter = in.toList
    val xCoord = iter.head match {
      case s1d: SensorMeasurement1D => s1d.x
      case s2d: SensorMeasurement2D => s2d.x
    }

    var breezeVector = BreezeVector.zeros[Double](featureCount)
    (1 to featureCount).foreach{ x =>
      breezeVector(x - 1) = Double.NaN
    }

    var counter = 0
    var measures = ""
    iter.foreach(measure => {
      val varKey: String = measure match {
        case s1d: SensorMeasurement1D => "C" + s1d.slice.head
        case s2d: SensorMeasurement2D => "C" + s2d.slice.head + "y" + s2d.y.toInt
      }
      val convertedIndex: Int = measure match {
        case s1d: SensorMeasurement1D => FeatureConversion.conversionMapping.getOrElse("C" + s1d.slice.head, -1)
        case s2d: SensorMeasurement2D =>
          var x = -1

          if (FeatureConversion.conversionMapping.exists(x => x._1 == "C" + s2d.slice.head + "y" + s2d.y.toInt)) {
            x = FeatureConversion.conversionMapping.getOrElse("C" + s2d.slice.head + "y" + s2d.y.toInt, -1)
          }
          else {
            if (FeatureConversion.conversionMapping.exists(x => x._1 == "C" + s2d.slice.head)) {
              x = FeatureConversion.conversionMapping.getOrElse("C" + s2d.slice.head, -1)
            }
          }
          x
      }
      if (convertedIndex > 0) {
        counter += 1
        measures += (varKey + ", ")
        breezeVector(convertedIndex - 1) = measure.data.head._2
      }
    })
    val vector = new DenseVector(breezeVector.toArray)

    val sensor: SensorMeasurement = SensorMeasurement((iter.head.coilId, xCoord), in.head.slice, vector)
    val ev: StreamEventWithPos[(Long, Double)] = sensor

    out.collect(Left(ev))
  }
}

class LassoOperation(
    targetVariable: String, workerParallelism: Int,
    psParallelism: Int, pullLimit: Int,
    featureCount: Int, rangePartitioning: Boolean,
    allowedFlatnessLateness: Long, allowedRealtimeLateness: Long, iterationWaitTime: Long) extends Serializable {

  def getKeyByCoilAndX(mesurement: CoilMeasurement): (Int, Double) = {
    val xCoord = mesurement match {
      case s1d: SensorMeasurement1D => s1d.x
      case s2d: SensorMeasurement2D => s2d.x
    }
    (mesurement.coilId, xCoord)
  }

  /**
    * Launch the Lasso operation for a given variable.
    *
    * @param measurementStream The input measurements stream.
    * @param flatnessStream The input flatness values stream.
    * @return A data stream of [[LassoResult]].
    */
  def runLasso(measurementStream: DataStream[CoilMeasurement],
               flatnessStream:DataStream[CoilMeasurement]): DataStream[LassoResult] = {

    val lasso = new LassoDelayedFeedbacks
    val varId: Int = targetVariable.replace("C", "").toInt

    implicit def transformStreamImplementation[T <: LassoStreamEvent] = {
      new LassoDFStreamTransformOperation[T](workerParallelism, psParallelism, pullLimit, featureCount,
        rangePartitioning, iterationWaitTime, allowedFlatnessLateness)
    }

    val processedFlatnessStream = flatnessStream.filter(x => x.slice.head == varId)
      .keyBy(x => x.coilId)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(allowedFlatnessLateness)))
      .process(new AggregateFlatnessValuesWindowFunction())

    val processedMeasurementStream = measurementStream.keyBy(x => getKeyByCoilAndX(x))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(allowedRealtimeLateness)))
      .process(new AggregateMeasurementValuesWindowFunction(featureCount))
    val connectedStreams = processedMeasurementStream.connect(processedFlatnessStream)

    val allEvents = connectedStreams.flatMap(new CoFlatMapFunction[LassoStreamEvent,
      LassoStreamEvent, LassoStreamEvent]() {

      override def flatMap1(value: LassoStreamEvent, out: Collector[LassoStreamEvent]): Unit = {
        out.collect(value)
      }

      override def flatMap2(value: LassoStreamEvent, out: Collector[LassoStreamEvent]): Unit = {
        out.collect(value)
      }
    }
    )

    val modelResults = lasso.transform[LassoStreamEvent, Either[((Long, Double), Double),
      (Int, LassoParam)] ](allEvents, ParameterMap.Empty)

    val onlyResults: DataStream[Option[LassoResult]] = modelResults.map{
      x =>
        x match {
          case Left(y) =>
            val coilId: Long = y._1._1
            val xCoord: Double = y._1._2
            val label: Double = y._2

            Some(new LassoResult(coilId, varId, xCoord, label))
          case Right(y) => None
        }
    }
    onlyResults.filter(x => x.nonEmpty).map(x => x.get)
  }
}
