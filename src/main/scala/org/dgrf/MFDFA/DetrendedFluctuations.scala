package org.dgrf.MFDFA
import org.dgrf.MFDFA.MFDFAImplicits._
class DetrendedFluctuations {
  var DFDataPointSet:List[DFDataPoint] = _

  def this(DFDataPointSet:List[DFDataPoint]) {
    this()
    this.DFDataPointSet = DFDataPointSet
  }
  def calculateSecondOrderResults (): SecondOrderResults = {
    val husrtExpt = DFDataPointSet.map(m=>(m.scaleSize,m.secondOrderRMS)).powerFit
    val secondOrderResults = SecondOrderResults(husrtExpt.slope,husrtExpt.SE,husrtExpt.RSquare)
    secondOrderResults
  }
  def calculateQOrderResults(): QOrderResults = {
    val scaleQRMSArray = DFDataPointSet.map(m=> m.qOrderRMSValues)
    val scaleQRMSTr = LinearSpace.qLinSpaceValues zip scaleQRMSArray.transpose
    val tq = scaleQRMSTr.map(m=>tqCalc(m))
    val hq = (tq zip tq.drop(1)).map({case (tqPrev,tqCurr)=> (tqCurr-tqPrev)/LinearSpace.qlinSpaceStep })

    val mfSpectrumHqvsDq = (hq,tq.dropRight(1),LinearSpace.qLinSpaceValues.dropRight(1)).zipped.toList.map(m=>prepMFSpectrum(m))
    val hqMin = mfSpectrumHqvsDq.map(m=>m.hq).reduceLeft(_ min _)
    val hqMax = mfSpectrumHqvsDq.map(m=>m.hq).reduceLeft(_ max _)
    val qOrderResults = new QOrderResults((hqMax-hqMin),mfSpectrumHqvsDq)
    qOrderResults
  }

  private def prepMFSpectrum(hqtqlinspace: (Double, Double, Double)): MFSpectrumDataPoint= {
    val hq = hqtqlinspace._1
    val tq = hqtqlinspace._2
    val qValue =  hqtqlinspace._3
    val dq = (qValue * hq) - tq
    val mfSpectrumDataPoint = MFSpectrumDataPoint(hq,dq)
    mfSpectrumDataPoint
  }
  private def tqCalc (Fq: (Double,List[Double])): Double = {
    val Hq = (LinearSpace.scaleSizeList zip Fq._2).map(m=>(m._1.toDouble,m._2)).powerFit.slope
    val qLinSpaceValue = Fq._1
    val tq = (Hq * qLinSpaceValue) -1
    tq
  }



}
