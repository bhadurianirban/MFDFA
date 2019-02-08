package org.dgrf.MFDFA

import org.dgrf.MFDFA.MFDFAUtil.logXBaseK

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object LinearSpace {
  //Linear space definication parameters
  var linspaceParams:LinspaceParams = _
  //Linear space results
  var qlinSpaceStep = 0.0
  var qLinSpaceValues:List[Double] = _
  var scaleSizeList:List[Int] = _

  def setupLinearSpace(linspaceParams:LinspaceParams = new LinspaceParams()): Unit = {
    this.linspaceParams = linspaceParams
    sliceUtil()
    qLinSpace()
    qLinSpaceValues.foreach(println)
  }
  private def sliceUtil ():Unit = {
    val exponentMin = MFDFAUtil.logXBaseK(linspaceParams.scaleMin)
    val exponentMax = MFDFAUtil.logXBaseK(linspaceParams.scaleMax)
    val sliceLinSpace = linSpace(exponentMin,exponentMax,linspaceParams.scaleCount)
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
    val qLinSpace = linSpace(linspaceParams.qLinSpaceStart,linspaceParams.qLinSpaceEnd,linspaceParams.qLinSpaceParitions)

    qLinSpaceValues = qLinSpace._2
    qlinSpaceStep = qLinSpace._1
  }
}
