package org.dgrf.MFDFA
import scala.math.log
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
object MFDFAUtil {
  var logBase:Double = 2
  var timeSeriesSize = 0
  var includeIntercept = true
  var qlinSpaceStep = 0.0
  var qValues:List[Double] = _
  var qLinSpaceStart:Double = -5.0
  var qLinSpaceEnd:Double = 5.0
  var qLinSpaceParitions:Int = 101
  var scaleMin:Double = 16
  var scaleMax:Double = 1024
  var scaleCount:Int = 19
  var scaleSizeList:List[Int] = _
  def sliceUtil ():Unit = {
    val exponentMin = logXBaseK(scaleMin)
    val exponentMax = logXBaseK(scaleMax)
    val sliceLinSpace = linSpace(exponentMin,exponentMax,scaleCount)
    //var scaleSizeArray = Array[Int](scaleCount)
    val scaleSizeArray = sliceLinSpace._2.map(m=> {
      val scaleSize = Math.pow(2, m).round.toInt
      scaleSize
    })

    scaleSizeList = scaleSizeArray
  }
  private def linSpace (start:Double,end:Double,scaleCount:Int):(Double, List[Double]) = {
    val lineSpace:Array[Double] = new Array[Double](scaleCount)
    val bigStart = BigDecimal(start)
    val bigEnd = BigDecimal(end)
    val bigDivision = BigDecimal(scaleCount - 1)
    val bigStep = ((bigEnd-bigStart)/(bigDivision)).setScale(16,RoundingMode.HALF_UP)

    var linValue = bigStart



    for (i<-0 to lineSpace.length-1) {

      lineSpace(i) = linValue.doubleValue()
      linValue = linValue + bigStep

    }

    (bigStep.doubleValue(),lineSpace.toList)
  }
  def qLinSpace (): Unit = {
    val qLinSpace = linSpace(qLinSpaceStart,qLinSpaceEnd,qLinSpaceParitions)
    qValues = qLinSpace._2
    qlinSpaceStep = qLinSpace._1
  }
  private def logXBaseK (x:Double): Double ={
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
