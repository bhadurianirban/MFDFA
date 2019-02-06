package org.dgrf.MFDFA

import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.dgrf.MFDFA.MFDFAImplicits._
import scala.math.log


class MFDFATimeSeries {

  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  var sparkSession:SparkSession = _
  var MFDFATimeSeriesDataset:Dataset[Row] = _
  private var transformedSeries:Dataset[Row] = _


  def this (sparkSession:SparkSession,inputTimeSeries:Dataset[Row]) {
    this()
    this.sparkSession = sparkSession
    this.MFDFATimeSeriesDataset = inputTimeSeries

  }
  def extractFluctuations(): DetrendedFluctuations = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("id"))
      .setOutputCol("features")

    transformedSeries = assembler.transform(MFDFATimeSeriesDataset)
    transformedSeries.persist()
    transformedSeries = transformedSeries.select(transformedSeries("id"),transformedSeries("yval").as("label"),transformedSeries("features"))


    //val scaleSizeList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)

    var secondAndQOrderFluctuations = LinearSpace.scaleSizeList.map(scaleSize=>processForEachScale(scaleSize))
    val DF = new DetrendedFluctuations(secondAndQOrderFluctuations)
    DF
    /*val husrtExpt = secondAndQOrderFluctuations.map(m=>(m.scaleSize,m.secondOrderRMS)).powerFit

    println("Hurst "+husrtExpt._1+" "+ husrtExpt._2)

    val scaleQRMSArray = secondAndQOrderFluctuations.map(m=> m.qOrderRMSValues)
    val scaleQRMSTr = LinearSpace.qLinSpaceValues zip scaleQRMSArray.transpose
    val tq = scaleQRMSTr.map(m=>gheu(m))
    val hq = (tq zip tq.drop(1)).map({case (tqPrev,tqCurr)=>((tqCurr-tqPrev)/LinearSpace.qlinSpaceStep)})
    println("hq "+hq.length+"tq "+tq.length+"qLin "+LinearSpace.qLinSpaceValues.length)
    val HqDq = (hq,tq.dropRight(1),LinearSpace.qLinSpaceValues.dropRight(1)).zipped.toList.map(m=>bheu(m))
    HqDq.foreach(println)*/

  }

  def processForEachScale (scaleSize:Int): SecondAndQOrderFluctuation = {

    val startEndIndexes = MFDFAUtil.getSliceStartEnd(scaleSize)
    val rmsListOfSlice = startEndIndexes.map(m => sliceByScaleAndCalcRMS(m))

    //rmsListOfSlice.foreach(println)
    //qValues.foreach(println)
    val qOrderRMSValues = LinearSpace.qLinSpaceValues.map(qValue=>calcqRMS(qValue,rmsListOfSlice))
    val secondOrderRMS = math.sqrt(rmsListOfSlice.map(math.pow(_, 2)).sum / rmsListOfSlice.size)
    //(scaleSize.toDouble,scaleRMS,qRMSresults)
    val secondAndQOrderFluctuation = SecondAndQOrderFluctuation(scaleSize ,secondOrderRMS ,qOrderRMSValues)
    secondAndQOrderFluctuation

  }
  def calcqRMS (qValue:Double,rmsListOfSlice:Array[Double]): Double = {
    val meanQPoweredRMS = rmsListOfSlice.map(rms=>calcqPoweredValue(rms,qValue)).meancalc
    var qRMS=0.0
    if (qValue == 0) {
      qRMS = Math.exp(0.5 * meanQPoweredRMS)
    } else {
      qRMS = Math.pow(meanQPoweredRMS, 1 / qValue)
    }
    qRMS

  }
  def calcqPoweredValue (rms:Double,qValue:Double): Double = {
    var qPoweredRMS = 0.0
    if (qValue == 0) {
      if (rms == 0) {
        qPoweredRMS = 0.0
      } else {
        qPoweredRMS = Math.log(Math.pow(rms,2))
      }
    } else {
      if (rms == 0) {
        qPoweredRMS = 0.0
      } else {
        qPoweredRMS = Math.pow(rms,qValue)
      }
    }
    qPoweredRMS
  }
  def sliceByScaleAndCalcRMS(startEndIndex:(Int,Int)): Double = {

    val inputTimeSeriesSlice  = transformedSeries.select("*").where(transformedSeries("id") between (startEndIndex._1,startEndIndex._2) )
    val lrModel = lr.fit(inputTimeSeriesSlice)
    val trainingSummary = lrModel.summary
    trainingSummary.rootMeanSquaredError
  }

}
