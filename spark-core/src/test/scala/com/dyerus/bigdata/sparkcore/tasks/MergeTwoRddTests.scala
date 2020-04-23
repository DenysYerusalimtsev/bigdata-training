package com.dyerus.bigdata.sparkcore.tasks

import com.dyerus.bigdata.sparkcore.BaseTest
import org.apache.spark.rdd.RDD

class MergeTwoRddTests extends BaseTest {
  behavior of MergeTwoRdd.getClass.getSimpleName

  it should "return the merged two RDDs" in {
    val firstRdd = List(
      Array("1", "first", "first", "first", "first", "first", "first", "1", "first"),
      Array("2", "second", "second", "second", "second", "second", "second", "2", "second"),
      Array("3", "third", "third", "third", "third", "third", "third", "3", "third")
    )

    val secondRdd = List(
      Array("1", "fourth", "fourth", "fourth", "fourth", "fourth", "fourth", "4", "fourth"),
      Array("2", "fifth", "fifth", "fifth", "fifth", "fifth", "fifth", "5", "fifth"),
      Array("3", "sixth", "sixth", "sixth", "sixth", "sixth", "sixth", "6", "sixth")
    )

    val inputFirstRdd = sc.parallelize(firstRdd)
    val inputSecondRdd = sc.parallelize(secondRdd)

    val expected = Array(
      (1,
        AirportRunaway(1, "first", "first", "first", "first"),
        AirportImpactOverlay(1, "fourth", "fourth", "fourth", "fourth")
      ),
      (2,
        AirportRunaway(2, "second", "second", "second", "second"),
        AirportImpactOverlay(2, "fifth", "fifth", "fifth", "fifth")
      ),
      (3,
        AirportRunaway(3, "third", "third", "third", "third"),
        AirportImpactOverlay(3, "sixth", "sixth", "sixth", "sixth")
      )
    )

    val rdd: RDD[(Int, (AirportRunaway, AirportImpactOverlay))] = MergeTwoRdd.mergeById(inputFirstRdd, inputSecondRdd)
    val actual = rdd.collect.deep

    actual == expected.deep
  }

  it should "return if first elements in two RDDs are same" in {
    val firstRdd = List(
      Array("1", "first", "first", "first", "first", "first", "first", "1", "first")
    )

    val secondRdd = List(
      Array("1", "fourth", "fourth", "fourth", "fourth", "fourth", "fourth", "4", "fourth")
    )

    val inputFirstRdd = sc.parallelize(firstRdd)
    val inputSecondRdd = sc.parallelize(secondRdd)

    val expected: List[(Int, AirportRunaway, AirportImpactOverlay)] = Array(
      (1,
        AirportRunaway(1, "first", "first", "first", "first"),
        AirportImpactOverlay(1, "fourth", "fourth", "fourth", "fourth")
      )
    ).toList

    val rdd: RDD[(Int, (AirportRunaway, AirportImpactOverlay))] = MergeTwoRdd.mergeById(inputFirstRdd, inputSecondRdd)
    val actual: List[(Int, (AirportRunaway, AirportImpactOverlay))] = rdd.collect.toList

    actual.head.canEqual(expected.head)
  }
}
