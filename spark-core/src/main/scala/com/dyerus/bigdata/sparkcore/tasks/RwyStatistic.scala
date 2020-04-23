package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

import scala.util.Try

object RwyStatistic {
  def calculateStatistic(ndHubRdd: RDD[Array[String]]): FullRwyStatistic = {
    val rwyLenRdd: RDD[RwyLen] = ndHubRdd.map(f => {
      val len: Double = Try(f(7).toDouble).getOrElse(0)
      RwyLen(rwyLen = len)
    })

    println(s"${RwyStatistic.getClass.getSimpleName} finished with result:")
    println(s"sum ${calculateSum(rwyLenRdd)}")
    println(s"avg ${calculateAvg(rwyLenRdd)}")
    println(s"max ${findMax(rwyLenRdd).rwyLen}")
    println(s"min ${findMin(rwyLenRdd).rwyLen}")

    FullRwyStatistic(
      calculateSum(rwyLenRdd),
      calculateAvg(rwyLenRdd),
      findMin(rwyLenRdd).rwyLen,
      findMax(rwyLenRdd).rwyLen)
  }

  private def calculateSum(rdd: RDD[RwyLen]): Double = rdd.map(_.rwyLen).sum()

  private def calculateAvg(rdd: RDD[RwyLen]): Double = calculateSum(rdd) / rdd.count()

  private implicit val ordering: Ordering[RwyLen] = Ordering[Double].on(x => x.rwyLen)

  private def findMax(rdd: RDD[RwyLen]): RwyLen = rdd.max()

  private def findMin(rdd: RDD[RwyLen]): RwyLen = rdd.filter(_.rwyLen >= 0).min()
}
