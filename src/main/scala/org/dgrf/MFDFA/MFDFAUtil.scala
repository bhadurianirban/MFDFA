package org.dgrf.MFDFA
import scala.math.log
import scala.math.ceil
object MFDFAUtil {
  var logBase:Double = 2
  var timeSeriesSize = 0
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
  def logXBaseK (x:Double): Double ={
    val logResult = log(x)/log(logBase)
    logResult
  }

  def getSliceStartEnd (sliceSize:Int):Array[(Int,Int)]= {
    
  }

}
