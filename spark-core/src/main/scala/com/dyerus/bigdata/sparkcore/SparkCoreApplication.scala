package com.dyerus.bigdata.sparkcore

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCoreApplication extends App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\Denis.Yerusalimtsev\\Downloads")

  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val df: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .load("C:\\Users\\Denis.Yerusalimtsev\\Downloads\\NDHUB.AirportRunways.csv")

  df.show()
}
