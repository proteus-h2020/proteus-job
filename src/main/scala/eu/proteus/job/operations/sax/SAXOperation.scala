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

package eu.proteus.job.operations.sax

import eu.proteus.job.kernel.SAXDictionaryTraining
import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.model.SensorMeasurement1D
import eu.proteus.job.operations.data.model.SensorMeasurement2D
import eu.proteus.job.operations.data.results.SAXResult
import eu.proteus.solma.sax.SAX
import eu.proteus.solma.sax.SAXDictionary
import eu.proteus.solma.sax.SAXPrediction
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.operators.AbstractStreamOperator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * SAX operation for the Proteus Job. The Job requires a pre-existing model for the SAX and SAX
 * dictionary for the specified variable.
 *
 * @param modelPath The path where the SAX dictionary models are stored.
 * @param targetVariable The target variable.
 * @param alphabetSize The SAX alphabet size.
 * @param paaFragmentSize The SAX PAA fragment size.
 * @param wordSize The SAX word size.
 * @param numberWords The number of words used to compare the incoming streaming with the SAX dictionary.
 */
class SAXOperation(
  modelPath: String,
  targetVariable: String,
  alphabetSize: Int = 5,
  paaFragmentSize: Int = 3,
  wordSize : Int = 5,
  numberWords : Int = 5
) {

  /**
   * Obtain the number of points that are required to obtain a prediction.
   * @return The number of points.
   */
  def numberOfPointsInPrediction() : Long = {
    this.paaFragmentSize * this.wordSize * this.numberWords
  }

  /**
   * Launch the SAX + SAX dictionary operation for a given variable.
   * @param stream The input stream.
   * @return A data stream of [[SAXResult]].
   */
  def runSAX(
    stream: DataStream[CoilMeasurement]
  ) : DataStream[SAXResult] = {

    // Define the SAX transformation.
    val saxParams = SAXDictionaryTraining.getSAXParameter(this.targetVariable)
    val sax = new SAX()
      .setAlphabetSize(this.alphabetSize)
      .setPAAFragmentSize(this.paaFragmentSize)
      .setWordSize(this.wordSize)
    sax.loadParameters(saxParams._1, saxParams._2)

    val varIndex = this.targetVariable.replace("C", "").toInt

    val transformOperator : OneInputStreamOperator[CoilMeasurement, (Double, Int)] = new SAXInputStreamOperator

    val filteredStream = stream.filter(_.slice.head == varIndex)
    filteredStream.name("filteredStream")

    val xValuesStream = this.processJoinStream(filteredStream)
    val transformedFilteredStream = filteredStream.transform("transform-to-SAX-input", transformOperator)
    val saxTransformation = sax.transform(transformedFilteredStream)

    // Define the SAX dictionary transformation.
    val dictionary = new SAXDictionary
    dictionary.loadModel(this.modelPath, this.targetVariable)
    dictionary.setNumberWords(this.numberWords)

    val dictionaryMatching = dictionary.predict(saxTransformation)
    val result = this.joinResult(dictionaryMatching, xValuesStream)
    //result.print()
    result
  }

  /**
   * From the filtered stream for the given variable, obtain the first and last X values involved
   * in the window.
   * @param filteredStream A filtered stream with the value and the coil identifier.
   * @return A DataStream with the starting X, the ending X, and the coil id.
   */
  def processJoinStream(filteredStream: DataStream[CoilMeasurement]) : DataStream[(Double, Double, Int)] = {

    val reduceXFunction = new WindowFunction[CoilMeasurement, (Double, Double, Int), Int, GlobalWindow] {
      override def apply(
        key: Int,
        window: GlobalWindow,
        input: Iterable[CoilMeasurement],
        out: Collector[(Double, Double, Int)]): Unit = {
        val maxMin = input.foldLeft((Double.MaxValue, Double.MinValue)){
          (acc, cur) => {

            val currentX = cur match {
              case s1d: SensorMeasurement1D => s1d.x
              case s2d: SensorMeasurement2D => s2d.x
            }

            (Math.min(acc._1, currentX), Math.max(acc._2, currentX))
          }
        }
        out.collect((maxMin._1, maxMin._2, key))
      }
    }

    val keyedStream = filteredStream
      .keyBy(_.coilId)
      .countWindow(this.numberOfPointsInPrediction())
      .apply(reduceXFunction)

    keyedStream.name("(MinX, MaxX, CoilId)")
    keyedStream
  }

  /**
   * Join the prediction results with the X window information.
   * @param saxResults The SAX results.
   * @param minMaxValues The minimum and maximum X values.
   * @return A [[SAXResult]].
   */
  def joinResult(
    saxResults: DataStream[SAXPrediction],
    minMaxValues: DataStream[(Double, Double, Int)]) : DataStream[SAXResult] = {

    val processFunction = new LinkOutputsFunction(this.targetVariable)

    saxResults.keyBy(_.key).connect(minMaxValues.keyBy(_._3)).flatMap(processFunction)

  }
}


class LinkOutputsFunction(targetVariable: String)
  extends RichCoFlatMapFunction[SAXPrediction, (Double, Double, Int), SAXResult] {

  @transient
  private var coordsMap: MapState[Int, mutable.Queue[(Double, Double)]] = _

  @transient
  private var orphanPredictions: MapState[Int, mutable.Queue[SAXPrediction]] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val cfg = getRuntimeContext.getExecutionConfig
    val keyType = createTypeInformation[Int]
    val valueType1 = createTypeInformation[mutable.Queue[(Double, Double)]]
    val valueType2 = createTypeInformation[mutable.Queue[SAXPrediction]]

    val coordsDescriptor = new MapStateDescriptor[Int, mutable.Queue[(Double, Double)]](
      "coords",
      keyType.createSerializer(cfg),
      valueType1.createSerializer(cfg)
    )
    val predictionsDescriptor = new MapStateDescriptor[Int, mutable.Queue[SAXPrediction]](
      "sax-predictions",
      keyType.createSerializer(cfg),
      valueType2.createSerializer(cfg)
    )
    this.coordsMap = getRuntimeContext.getMapState(coordsDescriptor)
    this.orphanPredictions = getRuntimeContext.getMapState(predictionsDescriptor)
  }

  /**
   * Join a prediction with the associated coordinates.
   * @param prediction The prediction information.
   * @param coords The coordinates.
   * @param out The output collector.
   */
  private def join(prediction: SAXPrediction, coords: (Double, Double), out: Collector[SAXResult]) : Unit = {
    out.collect(new SAXResult(
      prediction.key, this.targetVariable, coords._1, coords._2, prediction.classId, prediction.similarity))
  }

  override def flatMap2(in2: (Double, Double, Int), collector: Collector[SAXResult]): Unit = {
    val key = in2._3
    if(this.orphanPredictions.contains(key) && this.orphanPredictions.get(key).nonEmpty){
      val queue = this.orphanPredictions.get(key)
      val prediction = queue.dequeue()
      this.join(prediction, (in2._1, in2._2) , collector)
      // Required due to RocksDB bug
      this.orphanPredictions.put(key, queue)
    }else{
      val coordsQueue = if(this.coordsMap.contains(key)) {
        this.coordsMap.get(key)
      } else {
        mutable.Queue[(Double, Double)]()
      }
      coordsQueue.enqueue((in2._1, in2._2))
      this.coordsMap.put(key, coordsQueue)
    }

  }

  override def flatMap1(in1: SAXPrediction, collector: Collector[SAXResult]): Unit = {
    val key = in1.key
    if(this.coordsMap.contains(key) && this.coordsMap.get(key).nonEmpty){
      val queue = this.coordsMap.get(key)
      val coords = queue.dequeue()
      this.join(in1, coords, collector)
      // Required due to RocksDB bug
      this.coordsMap.put(key, queue)
    }else{
      val predictionQueue = if(this.orphanPredictions.contains(key)){
        this.orphanPredictions.get(key)
      }else{
        mutable.Queue[SAXPrediction]()
      }
      predictionQueue.enqueue(in1)
      this.orphanPredictions.put(key, predictionQueue)
    }

  }
}


class SAXInputStreamOperator
  extends AbstractStreamOperator[(Double, Int)]
  with OneInputStreamOperator[CoilMeasurement, (Double, Int)]{

  override def processElement(streamRecord: StreamRecord[CoilMeasurement]): Unit = {
    val result = (streamRecord.getValue.data.head._2, streamRecord.getValue.coilId)
    this.output.collect(new StreamRecord[(Double, Int)](result))
  }
}
