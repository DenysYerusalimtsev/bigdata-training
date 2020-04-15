package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

object RwyStatistic {
  def calculateStatistic(ndHubRdd: RDD[Array[String]]): Unit = {
    val rwyLenRdd: RDD[RwyLen] = ndHubRdd.map(f =>
      RwyLen(rwyLen = f(8).toInt))

    calculateSum(rwyLenRdd)
    calculateAvg(rwyLenRdd)
    findMax(rwyLenRdd)
    findMin(rwyLenRdd)
  }

  private def calculateSum(rdd: RDD[RwyLen]): Double = rdd.map(_.rwyLen).sum()

  private def calculateAvg(rdd: RDD[RwyLen]): Double = calculateSum(rdd) / rdd.count()

  private def findMax(rdd: RDD[RwyLen]): String = rdd.max.rwyLen

  private def findMin(rdd: RDD[RwyLen]): String = rdd.min.rwyLen
}
