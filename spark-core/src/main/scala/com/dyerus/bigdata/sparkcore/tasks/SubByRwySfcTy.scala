package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

object SubByRwySfcTy {
  def calculateStatistic(ndHubRdd: RDD[Array[String]]): Unit = {
    val rwyRdd: RDD[RwyLenSfcTy] = ndHubRdd.map(f =>
      RwyLenSfcTy(rwyType = f(11), rwyLen = f(8).toInt))

    sumByType(rwyRdd, "AFSC")
    sumByType(rwyRdd, "GRVD")
  }

  private def sumByType(rdd: RDD[RwyLenSfcTy], rwyType: String): Unit = {
    val sum = rdd
      .filter(f => f.rwyType.toLowerCase == rwyType.toLowerCase)
      .map(_.rwyLen)
      .sum()

    RwyLenSfcTy(rwyType, sum)
  }
}
