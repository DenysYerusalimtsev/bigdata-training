package com.dyerus.bigdata.sparkcore.tasks

import com.dyerus.bigdata.sparkcore.BaseTest

class RwyStatisticTests extends BaseTest {
  behavior of RwyStatistic.getClass.getSimpleName

  it should "return the correct Rwy Statistic in RDD" in {
    val rdd = List(
      Array("first", "first", "first", "first", "first", "first", "first", "1", "first"),
      Array("second", "second", "second", "second", "second", "second", "second", "2", "second"),
      Array("third", "third", "third", "third", "third", "third", "third", "3", "third")
    )
    val input = sc.parallelize(rdd)

    val expectedMax = 3
    val expectedMin = 1
    val expectedSum = 6
    val expectedAvg = 2


    val actual: FullRwyStatistic = RwyStatistic.calculateStatistic(input)

    actual.max shouldBe expectedMax
    actual.min shouldBe expectedMin
    actual.sum shouldBe expectedSum
    actual.avg shouldBe expectedAvg
  }

  it should "return the 0 for all statistic RDD" in {
    val rdd = List(
      Array("first", "first", "first", "first", "first", "first", "first", "0", "first"),
      Array("second", "second", "second", "second", "second", "second", "second", "0", "second"),
      Array("third", "third", "third", "third", "third", "third", "third", "0", "third")
    )
    val input = sc.parallelize(rdd)

    val expected = 0

    val actual: FullRwyStatistic = RwyStatistic.calculateStatistic(input)

    actual.max shouldBe expected
    actual.min shouldBe expected
    actual.sum shouldBe expected
    actual.avg shouldBe expected
  }
}
