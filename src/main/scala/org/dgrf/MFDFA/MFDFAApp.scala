package org.dgrf.MFDFA

import org.apache.spark.sql.SparkSession

object MFDFAApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()
    val inputUniformfile = args(0)
    val MFDFA = new MFDFA(sparkSession).readTimeSeries(inputUniformfile).extractFluctuations().calculateQOrderResults()

    /* MFDFAUtil.qLinSpace()
    MFDFAUtil.qValues.foreach(println)
    MFDFAUtil.sliceUtil()
    MFDFAUtil.scaleSizeList.foreach(println)*/
  }
}
