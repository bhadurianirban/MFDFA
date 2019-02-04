package org.dgrf.MFDFA
import scala.math.log
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
object MFDFAUtil {
  var logBase:Double = 2
  var timeSeriesSize = 0
  var includeIntercept = true
  var linspaceStep = 0.0
  var qValues:List[Double] = _
  def sliceUtil (scaleMax:Double,scaleMin:Double,scaleCount:Int): Array[Int] = {
    val exponentMin = logXBaseK(scaleMin)
    val exponentMax = logXBaseK(scaleMax)

    val lineSpace:Array[Int] = new Array[Int](scaleCount)
    val step = (exponentMax - exponentMin)/(scaleCount-1)

    var linValue = exponentMin
    for (i<-0 to lineSpace.length-1) {
      lineSpace(i) = Math.pow(2, linValue).round.toInt
      linValue = linValue + step

    }
    lineSpace
  }
  def qLinSpace (start:Double,end:Double,scaleCount:Int): Unit = {
    val lineSpace:Array[Double] = new Array[Double](scaleCount)
    val bigStart = BigDecimal(start)
    val bigEnd = BigDecimal(end)
    val bigDivision = BigDecimal(scaleCount - 1)
    val bigStep = ((bigEnd-bigStart)/(bigDivision)).setScale(16,RoundingMode.HALF_UP)
    linspaceStep = bigStep.doubleValue()
    var linValue = bigStart



    for (i<-0 to lineSpace.length-1) {

      lineSpace(i) = linValue.doubleValue()
      linValue = linValue + bigStep

    }

    qValues = lineSpace.toList
  }
  def logXBaseK (x:Double): Double ={
    val logResult = log(x)/log(logBase)
    logResult
  }

  def getSliceStartEnd (sliceSize:Int):Array[(Int,Int)]= {
    val sliceCount = timeSeriesSize/sliceSize
    val startEndIndexes:Array[(Int,Int)] = new Array[(Int, Int)](sliceCount)

    for (sliceNumber<-0 to sliceCount-1) {
      val startIndex = sliceSize*sliceNumber
      val endIndex = startIndex + sliceSize-1
      startEndIndexes(sliceNumber) = (startIndex,endIndex)
    }

    startEndIndexes
  }

}
