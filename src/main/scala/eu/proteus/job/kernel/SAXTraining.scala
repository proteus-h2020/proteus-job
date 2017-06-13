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

  import java.util.function.Consumer
  import java.util.{ArrayList => JArrayList}

  import eu.proteus.solma.sax.SAX
  import grizzled.slf4j.Logger
  import org.apache.flink.api.java.utils.ParameterTool
  import org.apache.flink.api.scala.DataSet
  import org.apache.flink.api.scala.ExecutionEnvironment
  import org.apache.flink.api.scala.createTypeInformation


  /**
   * This class trains the SAX model using all the variables in the PROTEUS dataset. The trained
   * parameters will be stored in a CSV file that can be loaded whenever a new variable is going to
   * be passed through SAX without the need to training each time.
   */

  object SAXTraining {

    private [kernel] val Log = Logger(getClass)

    /**
     * Set of variables that will be used for the training.
     */
    val Variables : Array[String] = Array(
      "C0001", "C0002", "C0003", "C0004", "C0005",
      "C0006", "C0007", "C0008", "C0009", "C0010",
      "C0012", "C0013", "C0014", "C0015", "C0016",
      "C0017", "C0018", "C0019", "C0020", "C0021",
      "C0022", "C0023", "C0024", "C0025", "C0026",
      "C0027", "C0034", "C0044"
      )

    /**
     * Print the usage of this program.
     */
    def printUsage() : Unit = {
      System.out.println("SAX Training Job")
      System.out.println("This job trains the SAX algorithm for all variables")
      System.out.println("Parameters:")
      System.out.println("--time-series-path\tThe RAW time series file")
    }

    /**
     * Entry point for launching the training job.
     *
     * Example of the arguments:
     * --time-series-path hdfs://localhost:9000/proteus/heterogeneous/final_full.csv
     *
     * @param args The arguments.
     */
    def main(args: Array[String]): Unit = {

      var parameters: Option[ParameterTool] = None
      var timeSeriesFilePath : Option[String] = None

      try {
        parameters = Some(ParameterTool.fromArgs(args))
        timeSeriesFilePath = Some(parameters.get.getRequired("time-series-path"))
      } catch {
        case t: Throwable =>
          Log.error("Error parsing the command line!")
          SAXTraining.printUsage
          System.exit(-1)
      }

      try {
        val saxTraining = new SAXTraining
        saxTraining.launchSAXTraining(timeSeriesFilePath.get)
      } catch {
        case t: Throwable =>
          Log.error("Failed to run the Proteus SAX Training Job!")
          Log.error(t.getMessage, t)
      }
    }

  }

  class SAXTraining {

    import SAXTraining.Log


    /**
     * Launch the training process.
     * @param timeSeriesPath The path to the time series file
     */
    def launchSAXTraining(timeSeriesPath : String) : Unit = {
      Log.info(s"Loading data from: ${timeSeriesPath}")
      val env = ExecutionEnvironment.getExecutionEnvironment
      val input = env.readTextFile(timeSeriesPath)

      // Transform the input into a dataset of tuples containing (variable id, value)
      val values : DataSet[(String, Double)] = input
        .filter(_ != "coil,x,y,varname,value")
        .map((line: String) => {
          val splits = line.split(",")
          (splits(splits.length - 2), splits(splits.length - 1).toDouble)
        })

      val result = new JArrayList[String]()

      SAXTraining.Variables.foreach((variable : String) => {
        Log.info(s"Training with ${variable}")
        val toTrain : DataSet[Double] = values.filter(_._1 == variable).map(_._2)
        val sax = new SAX()
        sax.fit(toTrain)
        val model = sax.getFittedParameters()
        Log.info(s"Fit for ${variable}: ${model}")
        result.add(s"${variable};${model}")
      })

      Log.info("Fitting results")
      result.forEach(new Consumer[String] {
        override def accept(t: String) = {
          Log.info(t)
        }
      })

    }



  }
