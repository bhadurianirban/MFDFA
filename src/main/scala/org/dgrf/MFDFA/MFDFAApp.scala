package org.dgrf.MFDFA

import org.apache.spark.sql.SparkSession

object MFDFAApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()
    val inputUniformfile = args(0)
    val MFDFA = new MFDFA(sparkSession,inputUniformfile).prepareCumulativeTimeSeries().calculateFQ()
    val RMSList = new List[Double] (5.35463089016756	,
      6.73560539035999	,
      8.69201848951751	,
      11.5039176578803	,
      15.0464955657423	,
      18.7446795815477	,
      22.2595168996536	,
      26.4224056079358	,
      32.8764614833029	,
      40.5209679871703	,
      49.9034320339342	,
      57.3193475039189	,
      60.4234124954542	,
      59.6954767181338	,
      60.8640384790367	,
      61.1753720502719	,
      61.456339319383	,
      61.2718851551659	,
      62.35332766197
    )
//    val linspace = MFDFAUtil.qLinSpace(-5.0,5.0,101)
//    linspace.foreach(println)
  }
}
