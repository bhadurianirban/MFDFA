package org.dgrf.MFDFA
import scala.math.log
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
object MFDFAUtil {
  var timeSeriesSize = 0
  var includeIntercept = true

  var logBase:Double = 2



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
