package org.dgrf.MFDFA

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

class MFDFA  {
  private val timeSeriesWithSeqfile = "/tmp/addedSeqFile.csv"
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
    val bufferedSource = Source.fromFile(UniformTimeSeriesFile)
    val outFileWriter = new File(timeSeriesWithSeqfile)
    val bw = new BufferedWriter(new FileWriter(outFileWriter))
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
    println("Mean: "+seriesMean+" "+sumValues+" "+lineCounter)
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
