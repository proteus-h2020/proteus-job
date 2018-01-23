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

import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.results.LassoResult
import eu.proteus.solma.events.{StreamEventLabel, StreamEventWithPos}
import eu.proteus.solma.lasso.Lasso.{LassoModel, LassoParam}
import eu.proteus.solma.lasso.{LassoDFPredictOperation, LassoDFStreamTransformOperation, LassoDelayedFeedbacks, LassoModelBuilder}
import eu.proteus.solma.lasso.LassoStreamEvent.LassoStreamEvent
import eu.proteus.solma.lasso.algorithm.LassoParameterInitializer.initConcrete
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
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
    val poses: List[Double] = iter.map(x => x.slice.head.toDouble)
    val labels: DenseVector = new DenseVector(iter.map(x => x.data(x.slice.head)).toArray)
    val flat: FlatnessMeasurement =
      FlatnessMeasurement(poses, iter.head.coilId, labels, in.head.slice, in.head.data)
    val ev: StreamEventLabel[Long, Double] = flat
    out.collect(Right(ev))
  }

}

class LassoOperation(
    targetVariable: String, workerParallelism: Int,
    psParallelism: Int, pullLimit: Int,
    featureCount: Int, rangePartitioning: Boolean,
    allowedLateness: Long, iterationWaitTime: Long) extends Serializable {

  /**
    * Launch the Lasso operation for a given variable.
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
        rangePartitioning, iterationWaitTime, allowedLateness)
    }

    implicit def predictImplementation[K <: LassoStreamEvent] = {
      new LassoDFPredictOperation[K](workerParallelism, psParallelism, pullLimit, featureCount,
        rangePartitioning, iterationWaitTime, allowedLateness)
    }

    val processedFlatnessStream = flatnessStream.filter(x => x.slice.head != varId).keyBy(x => x.coilId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(allowedLateness)))
      .process(new AggregateFlatnessValuesWindowFunction())

    def toLassoStreamEvent(in: CoilMeasurement): LassoStreamEvent = {
      val coilID = in.coilId
      val xCoord = in.slice.head
      val vector = in.data
      val slice = in.slice
      val ev: StreamEventWithPos[(Long, Double)] = SensorMeasurement((coilID, xCoord), slice, vector)
      Left(ev)
    }

    val processedMeasurementStream = measurementStream.map{x => toLassoStreamEvent(x)}

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

    val unlabeledVectors: DataStream[LassoStreamEvent] = allEvents.map{
      x =>
        x match {
          case Left(ev) => Some(Left(ev))
          case Right(ev) => None
        }
    }.filter(x => x.isEmpty).map(x => x.get)

    val predictResults = lasso.predict[LassoStreamEvent, Option[((Long, Double), Double)] ](unlabeledVectors,
      ParameterMap.Empty).filter(x => x.isEmpty).map(x => x.get)

    val modelResults = lasso.transform[LassoStreamEvent, Either[((Long, Double), Double),
      (Int, LassoParam)] ](allEvents, ParameterMap.Empty)

    modelResults.addSink(new RichSinkFunction[Either[((Long, Double), Double), (Int, LassoParam)]] {
      var lassoModel: LassoModel = initConcrete(1.0, 0.0, featureCount)(0)
      val modelBuilder = new LassoModelBuilder(lassoModel)

      override def invoke(value: Either[((Long, Double), Double), (Int, LassoParam)]): Unit = {
        value match {
          case Right((id, modelValue)) =>
            lassoModel = modelBuilder.add(id, modelValue)
          case Left(label) =>
          // prediction channel is deaf
        }
      }

      override def close(): Unit = {
      }
    })

    predictResults.map{
      x =>
        val coilId: Long = x._1._1
        val xCoord: Double = x._1._2
        val label: Double = x._2

        new LassoResult(coilId, varId, xCoord, label)
    }
  }
}
