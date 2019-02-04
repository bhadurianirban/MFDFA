package org.dgrf.MFDFA

import org.apache.commons.math3.stat.regression.SimpleRegression

object MFDFAImplicits {
  implicit class ImplDoubleVecUtils(values: Array[Double]) {

    def meancalc: Double = values.sum / values.length

  }
  implicit class ImplLinearReg (xyseries:Array[(Double,Double)]) {
    def regressionCalc :(Double,Double) = {
      val regset = new SimpleRegression(MFDFAUtil.includeIntercept)

      xyseries.foreach(m=>regset.addData(m._1,m._2))
      (regset.getSlope, regset.getIntercept)
    }
  }
}
