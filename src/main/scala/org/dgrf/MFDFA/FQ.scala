package org.dgrf.MFDFA

import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.dgrf.MFDFA.MFDFAImplicits._
import scala.math.log


class FQ {

  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  var sparkSession:SparkSession = _
  var inputTimeSeries:Dataset[Row] = _
  var transformedSeries:Dataset[Row] = _


  def this (sparkSession:SparkSession,inputTimeSeries:Dataset[Row]) {
    this()
    this.sparkSession = sparkSession
    this.inputTimeSeries = inputTimeSeries

  }
  def calculateFQ (scaleMax:Double=1024,scaleMin:Double=16,scaleCount:Int=19): Unit = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("id"))
      .setOutputCol("features")

    transformedSeries = assembler.transform(inputTimeSeries)
    transformedSeries.persist()
    transformedSeries = transformedSeries.select(transformedSeries("id"),transformedSeries("yval").as("label"),transformedSeries("features"))


    //val scaleSizeList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)

    val scaleRMSArray = MFDFAUtil.scaleSizeList.map(scaleSize=>processForEachScale(scaleSize))
    val husrtExpt = scaleRMSArray.map(m=>(m._1,m._2)).regressionCalc
    //scaleRMSArray(0)._3.foreach(println)
    //scaleRMSArray.foreach(println)
    //processForEachScale(1024)
    /*val regset = new SimpleRegression(MFDFAUtil.includeIntercept)

    scaleRMSArray.foreach(m=>regset.addData(m._1,m._2))*/
    println("Hurst "+husrtExpt._1+" "+ husrtExpt._2)

    val scaleQRMSArray = scaleRMSArray.map(m=> m._3)
    val scaleQRMSTr = MFDFAUtil.qLinSpaceValues zip scaleQRMSArray.transpose
    val tq = scaleQRMSTr.map(m=>gheu(m))
    val hq = (tq zip tq.drop(1)).map({case (tqPrev,tqCurr)=>((tqCurr-tqPrev)/MFDFAUtil.qlinSpaceStep)})
    hq.foreach(println)
  }
  def gheu(Fq: (Double,List[Double])): Double = {
    val Hq = (MFDFAUtil.scaleSizeList zip Fq._2).map(m=>(m._1.toDouble,m._2)).regressionCalc._1
    val qLinSpaceValue = Fq._1
    val tq = (Hq * qLinSpaceValue) -1
    tq
  }
  def processForEachScale (scaleSize:Int): (Double,Double,List[Double]) = {

    val startEndIndexes = MFDFAUtil.getSliceStartEnd(scaleSize)
    val rmsListOfSlice = startEndIndexes.map(m => sliceByScaleAndCalcRMS(m))

    //rmsListOfSlice.foreach(println)
    //qValues.foreach(println)
    val qOrderRMSValues = MFDFAUtil.qLinSpaceValues.map(qValue=>calcqRMS(qValue,rmsListOfSlice))
    val secondOrderRMS = math.sqrt(rmsListOfSlice.map(math.pow(_, 2)).sum / rmsListOfSlice.size)
    //(scaleSize.toDouble,scaleRMS,qRMSresults)
    (scaleSize ,secondOrderRMS ,qOrderRMSValues)

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
