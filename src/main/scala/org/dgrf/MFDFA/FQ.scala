package org.dgrf.MFDFA

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{DoubleType, LongType}

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
    //gheuts.show(10)
    //gheuts.printSchema()
    //transformedSeries.show(10)



    //val pred = lrModel.transform(transformedSeries)
    //pred.show(20)
    //val trainingSummary = lrModel.summary
    //println("coef"+lrModel.coefficients+"inter"+lrModel.intercept)

    //    println(s"numIterations: ${trainingSummary.totalIterations}")
    //    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //    trainingSummary.residuals.show()
    //    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //    println(s"r2: ${trainingSummary.r2}")

    val scaleSizeList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)
    val scaleRMSArray = scaleSizeList.map(scaleSize=>processForEachScale(scaleSize))
    scaleRMSArray.foreach(m => {
      val scaleGheu = m
      scaleGheu.foreach(gh=>println(gh._1+" "+gh._2))
    })
    //    val timeSeriesSlicedList = startEndIndexes.map(m => sliceTimeSeries(m))
    //    timeSeriesSlicedList.foreach(m=>gheu(m))
  }
  def processForEachScale (scaleSize:Int): Array[(Int,Double)] = {
    //println("scaleSize "+scaleSize)
    val startEndIndexes = MFDFAUtil.getSliceStartEnd(scaleSize)
    val rmsListOfSlice = startEndIndexes.map(m => sliceByScaleAndCalcRMS(m))
    val scaleRMSArray = rmsListOfSlice.map(rms=>(scaleSize,rms))
    scaleRMSArray
  }
  def sliceByScaleAndCalcRMS(startEndIndex:(Int,Int)): Double = {

    val inputTimeSeriesSlice  = transformedSeries.select("*").where(transformedSeries("id") between (startEndIndex._1,startEndIndex._2) )
    val lrModel = lr.fit(inputTimeSeriesSlice)
    val trainingSummary = lrModel.summary
    trainingSummary.rootMeanSquaredError
  }

}
