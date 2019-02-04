package org.dgrf.MFDFA

import org.apache.commons.math3.stat.regression.SimpleRegression

import scala.math.log

object MFDFAImplicits {
  implicit class ImplDoubleVecUtils(values: Array[Double]) {

    def meancalc: Double = values.sum / values.length

  }
  implicit class ImplLinearReg (xyseries:Array[(Double,Double)]) {
    def regressionCalc :(Double,Double) = {
      val regset = new SimpleRegression(MFDFAUtil.includeIntercept)

      xyseries.foreach(m=>{
        val x=  log(m._1) / log(MFDFAUtil.logBase)
        val y = log(m._2) / log(MFDFAUtil.logBase)
        regset.addData(x,y)
      })
      (regset.getSlope, regset.getIntercept)
    }
  }
}
