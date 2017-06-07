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

package eu.proteus.job.kernel

import eu.proteus.job.kernel.SAXTraining.getClass
import eu.proteus.solma.sax.SAX
import eu.proteus.solma.sax.SAXDictionary
import grizzled.slf4j.Logger
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.streaming.api.scala.DataStream


object SAXDictionaryTraining {

  private [kernel] val Log = Logger(getClass)

  /**
   * Get the SAX parameters that correspond to the training of SAX for a given variable.
   * @param varName The variable name.
   * @return A tuple containing mean and standard deviation.
   */
  def getSAXParameter(varName: String) : (Double, Double) = {
    varName match {
      case "C0001" => (-6.501700149121621, 712.5282254068459)
      case _ => throw new UnsupportedOperationException("Unrecognized variable")
    }
  }

  /**
   * Print the usage of this program.
   */
  def printUsage() : Unit = {
    System.out.println("SAX Dictionary training job")
    System.out.println("This job trains the SAX Dictionary for a given variable")
    System.out.println("Parameters:")
    System.out.println("--variable\tThe name of the variable")
    System.out.println("--training-coils\tThe number of training coils to be used")
    System.out.println("--flatness-classes-path\tThe path of the file associating classes")
    System.out.println("--time-series-path\tThe RAW time series file")
  }

  /**
   * Entry point for launching the training job.
   * @param args The arguments.
   */
  def main(args: Array[String]): Unit = {

    var parameters: Option[ParameterTool] = None
    var varName: Option[String] = None
    var trainingCoils : Option[Int] = None
    var flatnessClassesPath : Option[String] = None
    var timeSeriesFilePath : Option[String] = None

    try {
      parameters = Some(ParameterTool.fromArgs(args))
      varName = Some(parameters.get.getRequired("variable"))
      trainingCoils = Some(parameters.get.getRequired("training-coils").toInt)
      flatnessClassesPath = Some(parameters.get.getRequired("flatness-classes-path"))
      timeSeriesFilePath = Some(parameters.get.getRequired("time-series-path"))
    } catch {
      case t: Throwable =>
        Log.error("Error parsing the command line!")
        SAXDictionaryTraining.printUsage
        System.exit(-1)
    }

    try {
      val saxDictionaryTraining = new SAXDictionaryTraining
      saxDictionaryTraining.launchTraining(
        varName.get,
        trainingCoils.get,
        flatnessClassesPath.get,
        timeSeriesFilePath.get)
    } catch {
      case t: Throwable =>
        Log.error("Failed to run the Proteus SAX Training Job!")
        Log.error(t.getMessage, t)
    }
  }

}

case class TimeSeriesEvent(coilId: Long, varName: String, value: Double)

case class FlatnessClass(coilId: Long, flatnessValue: Double, classId: String)

/**
 * Use this class to train the SAX dictionary with a set of classes. Those classes will then
 * be used to compare streams of data and determine which class the data matches to.
 */
class SAXDictionaryTraining {

  import SAXDictionaryTraining.Log

  val env = ExecutionEnvironment.getExecutionEnvironment

  val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def registerTypes() : Unit = {
    val cfg = this.env.getConfig
    cfg.registerKryoType(classOf[TimeSeriesEvent])
    cfg.registerKryoType(classOf[FlatnessClass])

  }

  def launchTraining(
    varName: String,
    trainingCoils: Int,
    flatnessClassesPath: String,
    timeSeriesFilePath: String) : Unit = {

    Log.info(s"Loading class information from: ${flatnessClassesPath}")
    // Determine the number of classes.
    val flatnessInput = env.readTextFile(flatnessClassesPath)
    val timeSeriesInput = env.readTextFile(timeSeriesFilePath)

    val flatnessData = flatnessInput.map(line => {
      val splits = line.split(",")
      FlatnessClass(splits(0).toLong, splits(1).toDouble, splits(2))
    })

    val timeSeriesData = timeSeriesInput
      .filter(line => {line != "coil,x,y,varname,value" && !line.startsWith(",")})
      .map(line => {
        val splits = line.split(",")
        TimeSeriesEvent(splits(0).toLong, splits(splits.length - 2), splits(splits.length - 1).toDouble)
      })
      .filter(_.varName == varName)

    val classes : Seq[String] = flatnessData.map(_.classId).distinct().collect()
    Log.info(s"Available classes ${classes.mkString(", ")}")

    val saxParams = SAXDictionaryTraining.getSAXParameter(varName)
    val sax = new SAX().setAlphabetSize(8).setPAAFragmentSize(3).setWordSize(5)
    sax.loadParameters(saxParams._1, saxParams._2)
    val dictionary = new SAXDictionary

    classes.foreach(classId => {
      Log.info(s"Training for class: ${classId}")
      val setOfCoils : Seq[Long] = flatnessData
        .filter(_.classId == classId)
        .first(trainingCoils)
        .map(_.coilId).collect()

      val toSAXTrain = timeSeriesData
        .filter(data => setOfCoils.contains(data.coilId))
        .map(data => (data.value, data.coilId.toInt))
      // Values associated with coil ids for partitioning
      // TODO Make sure this follows the expected order.
      Log.info(s"Collecting filtered time series for coils: ${setOfCoils.mkString(", ")} on class: ${classId}")
      val filterResult : Seq[(Double, Int)] = toSAXTrain.collect()
      Log.info(s"Prepare the stream for the SAX transformation with ${filterResult.size} elements")
      val toSAXTrainStream : DataStream[(Double, Int)] = this.streamingEnv.fromCollection(filterResult)
      // Pass through SAX
      Log.info(s"SAX transformation on class: ${classId}")
      val saxResult : DataStream[(String, Int)] = sax.transform(toSAXTrainStream)
      // Transform into (value, classId) tuples
      val saxResultToTrain : DataSet[(String, String)] = this.env.fromCollection(saxResult.collect().toList).map(s => {(s._1, classId)})
      Log.info(s"Fitting dictionary on ${classId}")
      // Fit the class
      dictionary.fit(saxResultToTrain)

    })
    Log.info("Storing model")
    dictionary.storeModel("/tmp/", varName)

  }

}
