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

import java.io.File

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
      case "C0002" => (1915.7540755973803, 916.5125696962249)
      case "C0003" => (1582.1435820954591,1618.1181701398014)
      case "C0004" => (21629.553882954187,6338.7858564440485)
      case "C0005" => (21796.036289223506,6224.364571519489)
      case "C0006" => (21378.396344843568,6229.510579048364)
      case "C0007" => (93.03450308826994,1738.1863428728964)
      case "C0008" => (137.00874757756384,3153.27392439534)
      case "C0034" => (5082.531536086206,11963.636341625708)
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
    System.out.println("--model-storage-path\tThe path where the trained dictionary will be stored")
    System.out.println("--sax-alphabet-size\tThe alphabet size")
    System.out.println("--sax-paa-fragment-size\tThe PAA fragment size")
    System.out.println("--sax-word-size\tThe SAX word size")
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
    var modelStoragePath : Option[String] = None
    var alphabetSize : Option[Int] = None
    var paaFragmentSize : Option[Int] = None
    var wordSize : Option[Int] = None

    try {
      parameters = Some(ParameterTool.fromArgs(args))
      varName = Some(parameters.get.getRequired("variable"))
      trainingCoils = Some(parameters.get.getRequired("training-coils").toInt)
      flatnessClassesPath = Some(parameters.get.getRequired("flatness-classes-path"))
      timeSeriesFilePath = Some(parameters.get.getRequired("time-series-path"))
      modelStoragePath = Some(parameters.get.getRequired("model-storage-path"))
      alphabetSize = Some(parameters.get.getRequired("sax-alphabet-size").toInt)
      paaFragmentSize = Some(parameters.get.getRequired("sax-paa-fragment-size").toInt)
      wordSize = Some(parameters.get.getRequired("sax-word-size").toInt)

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
        timeSeriesFilePath.get,
        modelStoragePath.get,
        alphabetSize.get,
        paaFragmentSize.get,
        wordSize.get
      )
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

  /**
   * Execution environment.
   */
  val env = ExecutionEnvironment.getExecutionEnvironment

  /**
   * Streaming environment.
   */
  val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def registerTypes() : Unit = {
    val cfg = this.env.getConfig
    cfg.registerKryoType(classOf[TimeSeriesEvent])
    cfg.registerKryoType(classOf[FlatnessClass])
  }

  /**
   * Launch the training process. The timeseries will be processed on a class by class bases
   * and the final dictionary will be stored for future usage.
   *
   * @param varName The name of the target variable.
   * @param trainingCoils The number of coils used for training.
   * @param flatnessClassesPath The file path of the flatness classes.
   * @param timeSeriesFilePath The file path of the raw timeseries.
   * @param modelStoragePath The path where the trained models will be stored.
   * @param alphabetSize The SAX alphabet size.
   * @param paaFragmentSize The SAX PAA fragment size.
   * @param wordSize The SAX word size.
   */
  def launchTraining(
    varName: String,
    trainingCoils: Int,
    flatnessClassesPath: String,
    timeSeriesFilePath: String,
    modelStoragePath: String,
    alphabetSize: Int,
    paaFragmentSize: Int,
    wordSize: Int) : Unit = {

    this.env.setParallelism(1)
    this.streamingEnv.setParallelism(1)


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
    val sax = new SAX().setAlphabetSize(alphabetSize).setPAAFragmentSize(paaFragmentSize).setWordSize(wordSize)
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
      val saxResultToTrain : DataSet[(String, String)] = this.env
        .fromCollection(saxResult.collect().toList).map(s => {(s._1, classId)})
      Log.info(s"Fitting dictionary on ${classId}")
      // Fit the class
      dictionary.fit(saxResultToTrain)

    })
    Log.info("Storing model")
    dictionary.storeModel(modelStoragePath + File.separator +
      s"${trainingCoils}t_${alphabetSize}as_${paaFragmentSize}paa_${wordSize}ws" +
      File.separator, varName)

  }

}
