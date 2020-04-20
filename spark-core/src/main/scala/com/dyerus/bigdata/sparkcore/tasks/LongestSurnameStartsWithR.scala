package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

object LongestSurnameStartsWithR {
  def findLongestSurname(ndHubRdd: RDD[Array[String]]): String = {
    val passengers: RDD[PassengerLocation] = ndHubRdd.map(f =>
      PassengerLocation(
        locationId = f(1),
        fullName = f(3)))

    passengers.aggregate("")(
      (longest, person) => compare(longest, person.fullName),
      (longest, current) => compare(longest, current))
  }

  private def compare(first: String, second: String): String = {
    val startsWithR = (s: String) => s.startsWith("R")

    if (startsWithR(first) && !startsWithR(second)) first
    else if (!startsWithR(first) && startsWithR(second)) second
    else if (first.length > second.length) first else second
  }
}
