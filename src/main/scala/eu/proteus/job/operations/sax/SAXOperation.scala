package eu.proteus.job.operations.sax

import eu.proteus.job.kernel.SAXDictionaryTraining
import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.model.SensorMeasurement1D
import eu.proteus.job.operations.data.model.SensorMeasurement2D
import eu.proteus.job.operations.data.results.SAXResult
import eu.proteus.solma.sax.SAX
import eu.proteus.solma.sax.SAXDictionary
import eu.proteus.solma.sax.SAXPrediction
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.operators.AbstractStreamOperator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.util.Collector

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
    //filteredStream.print()

    val xValuesStream = this.processJoinStream(filteredStream)
    //xValuesStream.print()
    val transformedFilteredStream = filteredStream.transform("transform-to-SAX-input", transformOperator)
    val saxTransformation = sax.transform(transformedFilteredStream)

    //saxTransformation.print()

    // Define the SAX dictionary transformation.
    val dictionary = new SAXDictionary
    dictionary.loadModel(this.modelPath, this.targetVariable)
    dictionary.setNumberWords(this.numberWords)

    val dictionaryMatching = dictionary.predict(saxTransformation)

    //dictionaryMatching.print()
    val result = this.joinResult(dictionaryMatching, xValuesStream)
    result.print()

    result
  }

  /**
   * From the filtered stream for the given variable, obtain the first and last X values involved
   * in the window.
   * @param filteredStream A filtered stream with the value and the coil identifier.
   * @return A DataStream with the starting X, the ending X, and the coil id.
   */
  def processJoinStream(filteredStream: DataStream[CoilMeasurement]) : DataStream[(Int, Int, Int)] = {

    val reduceXFunction = new WindowFunction[CoilMeasurement, (Int, Int, Int), Int, GlobalWindow] {
      override def apply(key: Int, window: GlobalWindow, input: Iterable[CoilMeasurement], out: Collector[(Int, Int, Int)]): Unit = {
        val maxMin = input.foldLeft((Int.MaxValue, Int.MinValue)){
          (acc, cur) => {

            val currentX = cur match {
              case s1d: SensorMeasurement1D => s1d.x
              case s2d: SensorMeasurement2D => s2d.x
            }

            (Math.min(acc._1, currentX).toInt, Math.max(acc._2, currentX).toInt)
          }
        }
        out.collect((maxMin._1, maxMin._2, key))
      }
    }

    val keyedStream = filteredStream
      .keyBy(_.coilId)
      .countWindow(this.numberOfPointsInPrediction())
      .apply(reduceXFunction)

    keyedStream.name("(MinX, MaxX, CoilId")
    keyedStream
  }

  /**
   * Join the prediction results with the X window information.
   * @param saxResults The SAX results.
   * @param minMaxValues The minimum and maximum X values.
   * @return A [[SAXResult]].
   */
  def joinResult(saxResults: DataStream[SAXPrediction], minMaxValues: DataStream[(Int, Int, Int)]) : DataStream[SAXResult] = {

    val joinFunction = new JoinFunction[SAXPrediction, (Int, Int, Int), SAXResult] {
      override def join(in1: SAXPrediction, in2: (Int, Int, Int)): SAXResult = {
        new SAXResult(in1.key, in2._1, in2._2, in1.classId, in1.similarity)
      }
    }

    saxResults
      .join(minMaxValues)
      .where(_.key).equalTo(_._3)
      .window(GlobalWindows.create)
      .trigger(CountTrigger.of(1)).apply(joinFunction)

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
