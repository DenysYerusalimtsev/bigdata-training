package com.dyerus.bigdata.sparksql

import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql._

trait Elasticsearch {
  def writeToElastic[T](ds: Dataset[T], index: String): Unit =
    ds.toDF.saveToEs(index)

  def readFromElastic[T](index: String)(implicit spark: SparkSession): Dataset[T] = {

  }
}
