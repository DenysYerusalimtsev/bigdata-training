package com.dyerus.bigdata.sparkcore

import com.dyerus.bigdata.sparkcore.tasks._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCoreApplication extends App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\Denis.Yerusalimtsev\\Downloads")

  val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  /*
    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("C:\\Users\\Denis.Yerusalimtsev\\Downloads\\NDHUB.AirportRunways.csv")*/

  //df.show()
  val ndhubAirportFile: RDD[Array[String]] = spark.sparkContext
    .textFile("C:\\Users\\Denis.Yerusalimtsev\\Downloads\\NDHUB.AirportRunways.csv")
    .map(s => s.split(","))
    .skipHeader


  val loudounAirportImpact: RDD[Array[String]] = spark.sparkContext
    .textFile("C:\\Users\\Denis.Yerusalimtsev\\Downloads\\Loudoun_Airport_Impact_Overlay_Districts.csv")
    .map(s => s.split(","))
    .skipHeader

  LongestSurnameStartsWithR.findLongestSurname(ndhubAirportFile)
  SumByRwySfcType.calculateRwySumByTypes(ndhubAirportFile)
  RwyStatistic.calculateStatistic(ndhubAirportFile)

  val merged = MergeTwoRdd.mergeById(ndhubAirportFile, loudounAirportImpact)
  println(s"${MergeTwoRdd.getClass.getSimpleName} finished with result")
  merged.foreach(println)

  implicit class CsvRdd(rdd: RDD[Array[String]]) {
    def skipHeader: RDD[Array[String]] = {
      val header = rdd.first()
      val data = rdd.filter(row => !(row sameElements header))
      data
    }
  }
}
