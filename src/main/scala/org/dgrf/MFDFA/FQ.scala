package org.dgrf.MFDFA

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.commons.math3.stat.regression.SimpleRegression
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
    transformedSeries = transformedSeries.select(transformedSeries("id"),transformedSeries("yval").as("label"),transformedSeries("features"))


    val scaleSizeList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)
    val scaleRMSArray = scaleSizeList.map(scaleSize=>processForEachScale(scaleSize))
    scaleRMSArray.foreach(println)
    //val regset = new SimpleRegression(MFDFAUtil.includeIntercept)
    //scaleRMSArray.foreach(m=>regset.addData(m._1,m._2))
    //println("Hurst "+regset.getSlope+" "+ regset.getIntercept)

  }
  def processForEachScale (scaleSize:Int): (Double,Double) = {

    val startEndIndexes = MFDFAUtil.getSliceStartEnd(scaleSize)
    val rmsListOfSlice = startEndIndexes.map(m => sliceByScaleAndCalcRMS(m))
    val scaleRMS = math.sqrt(rmsListOfSlice.map(math.pow(_, 2)).sum / rmsListOfSlice.size)
    (scaleSize.toDouble,scaleRMS)
    //(log(scaleSize) / log(MFDFAUtil.logBase),log(scaleRMS)/(log(MFDFAUtil.logBase)))

  }
  def sliceByScaleAndCalcRMS(startEndIndex:(Int,Int)): Double = {

    val inputTimeSeriesSlice  = transformedSeries.select("*").where(transformedSeries("id") between (startEndIndex._1,startEndIndex._2) )
    val lrModel = lr.fit(inputTimeSeriesSlice)
    val trainingSummary = lrModel.summary
    trainingSummary.rootMeanSquaredError
  }

}
