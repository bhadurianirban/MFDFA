package org.dgrf.MFDFA

import org.dgrf.MFDFA.MFDFAUtil.logXBaseK

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object LinearSpace {
  //Linear space definication parameters
  var qLinSpaceStart:Double = -5.0
  var qLinSpaceEnd:Double = 5.0
  var qLinSpaceParitions:Int = 101
  var scaleMin:Double = 16
  var scaleMax:Double = 1024
  var scaleCount:Int = 19
  //Linear space results
  var qlinSpaceStep = 0.0
  var qLinSpaceValues:List[Double] = _
  var scaleSizeList:List[Int] = _

  def setupLinearSpace(): Unit = {
    sliceUtil()
    qLinSpace()
  }
  private def sliceUtil ():Unit = {
    val exponentMin = MFDFAUtil.logXBaseK(scaleMin)
    val exponentMax = MFDFAUtil.logXBaseK(scaleMax)
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
  private def qLinSpace (): Unit = {
    val qLinSpace = linSpace(qLinSpaceStart,qLinSpaceEnd,qLinSpaceParitions)

    qLinSpaceValues = qLinSpace._2
    qlinSpaceStep = qLinSpace._1
  }
}
