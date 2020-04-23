package com.dyerus.bigdata.sparkcore.tasks

import com.dyerus.bigdata.sparkcore.BaseTest
import org.apache.spark.rdd.RDD

class SumByRwySfcTypeTests extends BaseTest {
  behavior of SumByRwySfcType.getClass.getSimpleName

  it should "return the sum by Rwy type in RDD" in {
    val rdd = List(
      Array("AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "1", "AFSC", "AFSC", "AFSC", "AFSC"),
      Array("GRVD", "GRVD", "GRVD", "GRVD", "GRVD", "GRVD", "GRVD", "2", "GRVD", "GRVD", "GRVD", "GRVD"),
      Array("AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "AFSC", "3", "AFSC", "AFSC", "AFSC", "AFSC")
    )
    val input = sc.parallelize(rdd)

    val expectedRdd = List(
      RwySumByType("GRVD", 2),
      RwySumByType("AFSC", 4)
    )

    val expected: RDD[RwySumByType] = sc.parallelize(expectedRdd);
    val actual: RDD[RwySumByType] = SumByRwySfcType.calculateRwySumByTypes(input)

    actual.count shouldBe expected.count
    actual.take(0) shouldEqual  expected.take(0)
    actual.take(1) shouldEqual expected.take(1)
  }
}
