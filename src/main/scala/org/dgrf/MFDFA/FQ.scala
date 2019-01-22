package org.dgrf.MFDFA

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class FQ {
  var sparkSession:SparkSession = _
  var inputTimeSeries:Dataset[Row] = _
  def this (sparkSession:SparkSession,inputTimeSeries:Dataset[Row]) {
    this()
    this.sparkSession = sparkSession
    this.inputTimeSeries = inputTimeSeries
  }
  def calculateFQ (scaleMax:Double=1024,scaleMin:Double=16,scaleCount:Int=19): Unit = {
    val sliceList = MFDFAUtil.sliceUtil(scaleMax,scaleMin,scaleCount)
    sliceList.foreach(println)
  }
}
