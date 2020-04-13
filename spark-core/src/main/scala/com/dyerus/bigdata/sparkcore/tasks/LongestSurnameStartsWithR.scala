package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

object LongestSurnameStartsWithR {
  def findLongestSurname(ndHubRdd: RDD[Array[String]]): Unit = {
    val passengers: RDD[PassengerLocation] = ndHubRdd.map(f =>
      PassengerLocation(
        locationId = f(1),
        fullName = f(3)))

    val passengersWithR = passengers.filter(f => f.fullName.startsWith("R"))
    val longestR = findLongestSurname(passengersWithR)
    println(longestR)
  }

  private def findLongestSurname(rdd: RDD[PassengerLocation]): String = {
    val comparator = (first: String, second: String) =>
      if (first.length > second.length) first else second

    rdd.aggregate("")(
      (longest, person) => comparator(longest, person.fullName),
      (longest, current) => comparator(longest, current))
  }
}
