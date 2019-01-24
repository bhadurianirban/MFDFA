package org.dgrf.MFDFA

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{DoubleType, LongType}

class FQ {
  var sparkSession:SparkSession = _
  var inputTimeSeries:Dataset[Row] = _
  def this (sparkSession:SparkSession,inputTimeSeries:Dataset[Row]) {
    this()
    this.sparkSession = sparkSession
    this.inputTimeSeries = inputTimeSeries
  }
  def calculateFQ (scaleMax:Double=1024,scaleMin:Double=16,scaleCount:Int=19): Unit = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("xval"))
      .setOutputCol("features")

    val transformedSeries = assembler.transform(inputTimeSeries)
    val gheuts = transformedSeries.select(transformedSeries("yval").as("label"),transformedSeries("features"))
    gheuts.show(10)
    gheuts.printSchema()
    //transformedSeries.show(10)
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(gheuts)
    val pred = lrModel.transform(gheuts)
    pred.show(20)
    val trainingSummary = lrModel.summary
    println("coef"+lrModel.coefficients+"inter"+lrModel.intercept)
    /*println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")*/

/*    val sliceList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)
    val startEndIndexes = MFDFAUtil.getSliceStartEnd(sliceList(18))
    val timeSeriesSlicedList = startEndIndexes.map(m => sliceTimeSeries(m))
    timeSeriesSlicedList.foreach(m=>gheu(m))*/
  }
  def sliceTimeSeries(startEndIndex:(Int,Int)): Dataset[Row] = {

    val inputTimeSeriesSlice  = inputTimeSeries.select("*").where(inputTimeSeries("id") >= startEndIndex._1)
    inputTimeSeriesSlice
  }
  def gheu(timeSeriesSlice: Dataset[Row]): Unit = {

    timeSeriesSlice.show(2)
  }
}
