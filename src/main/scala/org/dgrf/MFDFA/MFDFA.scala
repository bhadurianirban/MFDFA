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
    readUniformDataFileAndAddSequence()
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
    bw.close()
    bufferedSource.close()
    val seriesMean = sumValues/lineCounter

    bufferedSource = Source.fromFile(timeSeriesWithSeqfile)
    outFileWriter = new File(timeSeriesWithSeqfile)
    bw = new BufferedWriter(new FileWriter(outFileWriter))
    for (line <- bufferedSource.getLines) {
      val lineValues = line.trim.split(",")
      val subtractMean = line(1).toDouble-seriesMean
      bw.write(lineValues(1)+","+subtractMean+"\n")

    }
    //println("Mean: "+seriesMean+" "+sumValues+" "+lineCounter)
    println("Written temp file "+timeSeriesWithSeqfile)
  }
  private def readSeqFileIntoDataset(): Unit = {
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("yval", DoubleType)
    ))
    val sqlContext = sparkSession.sqlContext
    println(timeSeriesWithSeqfile)
    inputTimeSeries = sqlContext.read.schema(schema).option("delimiter",",").csv("file://"+timeSeriesWithSeqfile)
  }
}
