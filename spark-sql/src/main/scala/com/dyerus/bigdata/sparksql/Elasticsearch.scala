package com.dyerus.bigdata.sparksql

import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql._

trait Elasticsearch {
  def writeToElastic[T](ds: Dataset[T], index: String): Unit =
    ds.writeStream
      .option("checkpointLocation", "/save/location")
      .format("es")
      .start(index)

  def readFromElastic[T](index: String)(implicit spark: SparkSession): Unit = {
  }
}
