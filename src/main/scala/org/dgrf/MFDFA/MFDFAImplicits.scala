package org.dgrf.MFDFA

object MFDFAImplicits {
  implicit class ImplDoubleVecUtils(values: Array[Double]) {

    def meancalc: Double = values.sum / values.length
  }
}
