package eu.proteus.job.operations.sax

import eu.proteus.job.kernel.SAXDictionaryTraining
import eu.proteus.job.operations.data.model.CoilMeasurement
import eu.proteus.job.operations.data.results.SAXResult
import eu.proteus.solma.sax.SAX
import eu.proteus.solma.sax.SAXDictionary
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.operators.AbstractStreamOperator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

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
  alphabetSize: Int = 2,
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
  ) : Unit = { //DataStream[SAXResult]

    // Define the SAX transformation.
    val saxParams = SAXDictionaryTraining.getSAXParameter(this.targetVariable)
    val sax = new SAX()
      .setAlphabetSize(this.alphabetSize)
      .setPAAFragmentSize(this.paaFragmentSize)
      .setWordSize(this.wordSize)
    sax.loadParameters(saxParams._1, saxParams._2)

    // TODO Check how to obtain the value.
    val varIndex = this.targetVariable.replace("C", "").toInt

    val transformOperator : OneInputStreamOperator[CoilMeasurement, (Double, Int)] = new SAXInputStreamOperator

    val filteredStream = stream
      .filter(_.slice.head == varIndex).transform("transform-to-SAX-input", transformOperator)
      //.filter(measurement : CoilMeasurement => {
      //  measurement.
      //})
      //.map(measurement => {
      //    (measurement.data(varIndex), measurement.coilId)
      //  })

    val saxTransformation = sax.transform(filteredStream)

    // Define the SAX dictionary transformation.
    val dictionary = new SAXDictionary
    dictionary.loadModel(this.modelPath, this.targetVariable)

    val dictionaryMatching = dictionary.predict(saxTransformation)

    dictionaryMatching.print()

  }

}

class SAXInputStreamOperator extends AbstractStreamOperator[(Double, Int)] with OneInputStreamOperator[CoilMeasurement, (Double, Int)]{
  override def processElement(streamRecord: StreamRecord[CoilMeasurement]): Unit = {
    val result = (streamRecord.getValue.data.head._2, streamRecord.getValue.coilId)
    this.output.collect(new StreamRecord[(Double, Int)](result))
  }
}
