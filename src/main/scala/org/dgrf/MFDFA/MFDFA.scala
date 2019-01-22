package org.dgrf.MFDFA

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

class MFDFA  {
  private val timeSeriesWithSeqfile = "/tmp/addedSeqFileTmp.csv"
  private val timeSeriesMFDFASeqfile = "/tmp/addedSeqFile.csv"
  var sparkSession:SparkSession = _
  var inputTimeSeries:Dataset[Row] = _
  var UniformTimeSeriesFile:String =_
  def this (sparkSession:SparkSession,UniformTimeSeriesFile:String) {
    this()
    this.sparkSession = sparkSession
    this.UniformTimeSeriesFile = UniformTimeSeriesFile

  }
  def prepareCumulativeTimeSeries(): FQ = {
    readUniformDataFileAndAddSequence()
    readSeqFileIntoDataset()
    val fq = new FQ (sparkSession,inputTimeSeries)
    fq
  }

  private def readUniformDataFileAndAddSequence(): Unit = {
    var bufferedSource = Source.fromFile(UniformTimeSeriesFile)
    var outFileWriter = new File(timeSeriesWithSeqfile)
    var bw = new BufferedWriter(new FileWriter(outFileWriter))
    var lineCounter:Int = 0
    var sumValues:Double =0
    for (line <- bufferedSource.getLines) {
      sumValues = line.toDouble +sumValues
      bw.write(lineCounter+","+line+"\n")
      lineCounter = lineCounter+1
    }
    MFDFAUtil.timeSeriesSize = lineCounter
    bw.close()
    bufferedSource.close()
    val seriesMean = sumValues/lineCounter

    bufferedSource = Source.fromFile(timeSeriesWithSeqfile)
    outFileWriter = new File(timeSeriesMFDFASeqfile)
    bw = new BufferedWriter(new FileWriter(outFileWriter))
    var cumLineValue = 0.0
    var prevCumLineValue = 0.0
    for (line <- bufferedSource.getLines) {
      val lineValues = line.trim.split(",")
      val subtractMean = lineValues(1).toDouble-seriesMean
      cumLineValue = prevCumLineValue+subtractMean
      bw.write(lineValues(0)+","+cumLineValue+"\n")
      prevCumLineValue = cumLineValue
    }

    bw.close()
    bufferedSource.close()
    //println("Mean: "+seriesMean+" "+sumValues+" "+lineCounter)

  }
  private def readSeqFileIntoDataset(): Unit = {
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("yval", DoubleType)
    ))
    val sqlContext = sparkSession.sqlContext

    inputTimeSeries = sqlContext.read.schema(schema).option("delimiter",",").csv("file://"+timeSeriesMFDFASeqfile)
  }
}
