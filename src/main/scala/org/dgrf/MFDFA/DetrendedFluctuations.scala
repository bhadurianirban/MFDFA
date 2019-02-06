package org.dgrf.MFDFA
import org.dgrf.MFDFA.MFDFAImplicits._
class DetrendedFluctuations {
  var secondAndQOrderFluctuations:List[SecondAndQOrderFluctuation] = _

  def this(secondAndQOrderFluctuations:List[SecondAndQOrderFluctuation]) {
    this()
    this.secondAndQOrderFluctuations = secondAndQOrderFluctuations
  }
  def calculateSecondOrderResults (): SecondOrderResults = {
    val husrtExpt = secondAndQOrderFluctuations.map(m=>(m.scaleSize,m.secondOrderRMS)).powerFit
    val secondOrderResults = SecondOrderResults(husrtExpt.slope,husrtExpt.SE,husrtExpt.RSquare)
    secondOrderResults
  }
  def calculateQOrderResults(): Unit = {
    val scaleQRMSArray = secondAndQOrderFluctuations.map(m=> m.qOrderRMSValues)
    val scaleQRMSTr = LinearSpace.qLinSpaceValues zip scaleQRMSArray.transpose
    val tq = scaleQRMSTr.map(m=>gheu(m))
    val hq = (tq zip tq.drop(1)).map({case (tqPrev,tqCurr)=> (tqCurr-tqPrev)/LinearSpace.qlinSpaceStep })
    println("hq "+hq.length+"tq "+tq.length+"qLin "+LinearSpace.qLinSpaceValues.length)
    val HqDq = (hq,tq.dropRight(1),LinearSpace.qLinSpaceValues.dropRight(1)).zipped.toList.map(m=>bheu(m))
    HqDq.foreach(println)
  }
  def bheu(hqtqlinspace: (Double, Double, Double)): (Double,Double )= {
    val hqValue = hqtqlinspace._1
    val tqValue = hqtqlinspace._2
    val qValue =  hqtqlinspace._3
    val Dq = (qValue * hqValue) - tqValue
    (hqValue,Dq)
  }
  def gheu(Fq: (Double,List[Double])): Double = {
    val Hq = (LinearSpace.scaleSizeList zip Fq._2).map(m=>(m._1.toDouble,m._2)).powerFit.slope
    val qLinSpaceValue = Fq._1
    val tq = (Hq * qLinSpaceValue) -1
    tq
  }



}
