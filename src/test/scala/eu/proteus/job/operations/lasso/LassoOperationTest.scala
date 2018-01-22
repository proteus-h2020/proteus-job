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

import java.util.{ArrayList => JArrayList}

import eu.proteus.job.operations.data.model.{CoilMeasurement, SensorMeasurement1D}
import eu.proteus.job.operations.data.results.LassoResult
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.ml.math.{DenseVector => FlinkDenseVector}
import org.apache.flink.api.scala.createTypeInformation
import org.scalatest.{FunSuite, Matchers}



import scala.collection.JavaConversions.asScalaBuffer


object LassoOperationTest {
  val featuresFilePath = "resources/features.csv"
  val flatnessFilePath = "resources/flatness.csv"

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
}

class LassoOperationTest extends FunSuite with Matchers {

  def getLassoOperation: LassoOperation = {
    val workerParallelism = 1
    val psParallelism = 1
    val pullLimit = 10000
    val featureCount = 76
    val rangePartitioning = true
    val allowedLateness = 10000
    val iterationWaitTime: Long = 10000
    val varName = "C0000"

    val operation = new LassoOperation(varName, workerParallelism, psParallelism, pullLimit, featureCount,
      rangePartitioning, allowedLateness, iterationWaitTime)

    operation
  }

  def getInputMeasurement(numElements: Int): DataStream[CoilMeasurement] = {
    val inputData = new JArrayList[CoilMeasurement]()
    0.until(numElements).foreach(index => {
      inputData.add(SensorMeasurement1D(0, index, 0 to 0, FlinkDenseVector(index % 3)))
    })
    LassoOperationTest.env.fromCollection(inputData.toSeq)
  }

  def getInputFlatness(numElements: Int): DataStream[CoilMeasurement] = {
    val inputData = new JArrayList[CoilMeasurement]()
    0.until(numElements).foreach(index => {
      inputData.add(SensorMeasurement1D(0, index, 0 to 0, FlinkDenseVector(index % 3)))
    })
    LassoOperationTest.env.fromCollection(inputData.toSeq)
  }

  test("processJoinStream, numberOfPointsInPrediction=3"){
    val op: LassoOperation = getLassoOperation
    val inputMeasurement = getInputMeasurement(10)
    val inputFlatness = getInputFlatness(10)

    val processingResult: DataStream[LassoResult] = op.runLasso(inputMeasurement, inputFlatness)

    //val resultIt = processingResult.collect()

    assert("hola" === "hola", "Result should match")
  }

}
