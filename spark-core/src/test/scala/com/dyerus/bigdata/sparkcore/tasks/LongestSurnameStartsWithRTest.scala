package com.dyerus.bigdata.sparkcore.tasks

import com.dyerus.bigdata.sparkcore.BaseTest

class LongestSurnameStartsWithRTest extends BaseTest {
  behavior of LongestSurnameStartsWithR.getClass.getSimpleName

  it should "return the longest surname in RDD" in {
    val surnames = List(
      Array("first", "first", "first", "first"),
      Array("second", "second", "second", "second"),
      Array("longest", "longest", "longest", "longest")
    )
    val input = sc.parallelize(surnames)
    val expected = "longest"

    val actual = LongestSurnameStartsWithR.findLongestSurname(input)

    actual shouldBe expected
  }

  it should "return empty string if input RDD is empty" in {
    val surnames: List[Array[String]] = List()
    val input = sc.parallelize(surnames)
    val expected = ""

    val actual = LongestSurnameStartsWithR.findLongestSurname(input)

    actual shouldBe expected
  }
}
