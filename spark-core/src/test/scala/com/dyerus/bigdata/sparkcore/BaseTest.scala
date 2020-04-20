package com.dyerus.bigdata.sparkcore

import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class BaseTest extends FlatSpec
  with Matchers with SharedSparkContext