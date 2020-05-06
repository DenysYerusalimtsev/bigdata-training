package com.dyerus.bigdata.sparksql

import com.dyerus.bigdata.sparkcore.SparkSetup
import org.apache.spark.sql.SparkSession

trait Spark extends SparkSetup {
  protected implicit val spark: SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("es.index.auto.create", "true")
    .config("spark.es.nodes", "http://prismelastic.northcentralus.azurecontainer.io")
    .config("spark.es.port", "9200")
    .config("spark.es.nodes.wan.only", "true")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")
}
