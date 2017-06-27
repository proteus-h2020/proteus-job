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

import java.util.{ArrayList => JArrayList}

import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.model.SensorMeasurement1D
import eu.proteus.job.operations.data.results.SAXResult
import eu.proteus.solma.sax.SAXPrediction
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.api.scala.createTypeInformation

import scala.collection.JavaConversions.asScalaBuffer
import org.scalatest.FunSuite
import org.scalatest.Matchers

/**
 * Test for the SAX + SAX dictionary operation.
 */
class SAXOperationTest  extends FunSuite with Matchers {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * Get the input data in the form of coil measurements.
   * @param numElements The number of elements to be generated.
   * @return A [[DataStream]] of [[CoilMeasurement]].
   */
  def getInputData(numElements : Int = 10) : DataStream[CoilMeasurement] = {
    val inputData = new JArrayList[CoilMeasurement]()
    0.until(numElements).foreach(index => {
      inputData.add(SensorMeasurement1D(0, index, 0 to 0, FlinkDenseVector(index % 3)))
    })
    val inputStream : DataStream[CoilMeasurement] = this.streamingEnv.fromCollection(inputData.toSeq)
    inputStream
  }

  /**
   * Build a SAX operation using the testing parameters.
   * @return A SAX operation.
   */
  def getSAXOperation() : SAXOperation = {
    val modelPath = ""
    val varName = "C0000"
    val alphabetSize = 3
    val paaFragmentSize = 1
    val wordSize = 1
    val numberWords = 3
    val operation = new SAXOperation(modelPath, varName, alphabetSize, paaFragmentSize, wordSize, numberWords)
    operation
  }

  test("processJoinStream, numberOfPointsInPrediction=3"){
    val numValues = 12
    val op = this.getSAXOperation()
    val inputData = this.getInputData(numValues)
    val processingResult : DataStream[(Double, Double, Int)] = op.processJoinStream(inputData)
    val resultIt : Iterator[(Double, Double, Int)] = processingResult.collect()
    val result = resultIt.toList
    val expected : Seq[(Int, Int, Int)] = List((0, 2, 0), (3, 5, 0), (6, 8, 0), (9, 11, 0))
    assert(result === expected, "Result should match")
  }

  test("joinResult"){
    val predictionInput : DataStream[SAXPrediction] = this.streamingEnv.fromCollection(
      Seq(
        SAXPrediction(0, "A", 1.0),
        SAXPrediction(0, "B", 1.0),
        SAXPrediction(0, "C", 1.0),
        SAXPrediction(0, "D", 1.0),
        SAXPrediction(0, "E", 1.0),
        SAXPrediction(0, "F", 1.0)
      )
    )

    val xValuesInput : DataStream[(Double, Double, Int)] = this.streamingEnv.fromCollection(
      Seq((0.0, 1.0, 0), (2.0, 3.0, 0), (4.0, 5.0, 0), (6.0, 7.0, 0), (8.0, 9.0, 0), (10.0, 11.0, 0))
    )

    val op = this.getSAXOperation()

    val join : DataStream[SAXResult] = op.joinResult(predictionInput, xValuesInput)
    val resultIt : Iterator[SAXResult] = join.collect()
    val result = resultIt.toList
    Console.println(result.mkString(", "))

  }

}
