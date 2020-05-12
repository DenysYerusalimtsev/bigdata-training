package com.dyerus.bigdata.sparksql

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import scala.reflect.runtime.universe.TypeTag

trait Elasticsearch {
  def writeToElastic[T](ds: Dataset[T], index: String): Unit =
    ds.writeStream
      .option("checkpointLocation", "/save/location")
      .format("es")
      .start(index)

  def readFromElastic[A <: Product : TypeTag](index: String)(implicit spark: SparkSession): Dataset[A] = {
    spark
      .read
      .format("es")
      .option("es.read.field.as.array.include", "participatingCountries")
      .load(index)
      .as[A](Encoders.product[A])
  }
}