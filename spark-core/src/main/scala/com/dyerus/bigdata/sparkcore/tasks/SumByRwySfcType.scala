package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

import scala.util.Try

object SumByRwySfcType {
  def calculateRwySumByTypes(ndHubRdd: RDD[Array[String]]): Unit = {
    val rwyRdd: RDD[RwyLenSfcTy] = ndHubRdd
      .map(f => {
        val len: Double = Try(f(7).toDouble).getOrElse(0)

        RwyLenSfcTy(
          rwyType = f(10),
          rwyLen = len)
      })

    val sum = sumByType(rwyRdd, Array("AFSC", "GRVD"))

    println(s"${SumByRwySfcType.getClass.getSimpleName} finished with result:")
    sum.foreach(println)
  }

  private def sumByType(rdd: RDD[RwyLenSfcTy], rwyTypes: Seq[String]): RDD[(String, Double)] = {
    val rwyTypesLower: Seq[String] = rwyTypes.map(_.toLowerCase)

    val sum: RDD[(String, Double)] = rdd
      .filter(f => rwyTypesLower.contains(f.rwyType.toLowerCase))
      .map(rwy => (rwy.rwyType, rwy.rwyLen))
      .reduceByKey((first, second) => first + second)

    sum
  }
}
