package com.dyerus.bigdata.sparkcore.tasks

import org.apache.spark.rdd.RDD

object MergeTwoRdd {
  def mergeById(ndHubRdd: RDD[Array[String]], loudounAirportImpact: RDD[Array[String]]): RDD[(Int, (AirportRunaway, AirportImpactOverlay))] = {
    val airportRunawayRdd = ndHubRdd.map(item => AirportRunaway(
      id = item(0).toInt,
      locId = item(1),
      siteNo = item(2),
      fullName = item(3),
      faaSt = item(4))
    )

    val airportImpactOverlayRdd = loudounAirportImpact.map(item => AirportImpactOverlay(
      id = item(0).toInt,
      ldNoisezone = item(1),
      ldLocation = item(2),
      ldDescriptionOrd = item(3),
      ldUpdDate = item(4)))

    val airportRunawayPaired = airportRunawayRdd.map(item => (item.id, item))
    val loudounAirportImpactPaired = airportImpactOverlayRdd.map(item => (item.id, item))

    val joined: RDD[(Int, (AirportRunaway, AirportImpactOverlay))] =
      airportRunawayPaired.join(loudounAirportImpactPaired)

    joined
  }
}
