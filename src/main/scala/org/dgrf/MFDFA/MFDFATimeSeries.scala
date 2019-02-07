package org.dgrf.MFDFA

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.dgrf.MFDFA.MFDFAImplicits._


class MFDFATimeSeries {

  val lr: LinearRegression = new LinearRegression()
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

    val DFDataPointSet = LinearSpace.scaleSizeList.map(scaleSize=>extractFluctuationsForScale(scaleSize))
    val DF = new DetrendedFluctuations(DFDataPointSet)
    DF


  }

  private def extractFluctuationsForScale(scaleSize:Int): DFDataPoint = {

    val startEndIndexes = MFDFAUtil.getSliceStartEnd(scaleSize)
    val fluctuationDataPointsForScale = startEndIndexes.map(m => sliceByScaleAndCalcRMS(m))

    val qOrderFluctuationDataPoint = LinearSpace.qLinSpaceValues.map(qValue=>calcqRMS(qValue,fluctuationDataPointsForScale))
    val secondOrderFluctuationDataPoint = math.sqrt(fluctuationDataPointsForScale.map(math.pow(_, 2)).sum / fluctuationDataPointsForScale.length)

    val dfDataPoint = DFDataPoint(scaleSize ,secondOrderFluctuationDataPoint ,qOrderFluctuationDataPoint)
    dfDataPoint

  }
  private def calcqRMS (qValue:Double,rmsListOfSlice:Array[Double]): Double = {
    val meanQPoweredRMS = rmsListOfSlice.map(rms=>calcqPoweredValue(rms,qValue)).meancalc
    var qRMS=0.0
    if (qValue == 0) {
      qRMS = Math.exp(0.5 * meanQPoweredRMS)
    } else {
      qRMS = Math.pow(meanQPoweredRMS, 1 / qValue)
    }
    qRMS

  }
  private def calcqPoweredValue (rms:Double,qValue:Double): Double = {
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
  private def sliceByScaleAndCalcRMS(startEndIndex:(Int,Int)): Double = {

    val inputTimeSeriesSlice  = transformedSeries.select("*").where(transformedSeries("id") between (startEndIndex._1,startEndIndex._2) )
    val lrModel = lr.fit(inputTimeSeriesSlice)
    val trainingSummary = lrModel.summary
    trainingSummary.rootMeanSquaredError
  }

}
