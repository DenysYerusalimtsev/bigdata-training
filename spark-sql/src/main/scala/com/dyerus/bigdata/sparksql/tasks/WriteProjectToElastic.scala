package com.dyerus.bigdata.sparksql.tasks

import com.dyerus.bigdata.sparksql.{Elasticsearch, Spark}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery

object WriteProjectToElastic extends Elasticsearch with Spark {
  def run()(implicit spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "project_members_added")
      .option("failOnDataLoss", "false")
      .load()

    val streamValue: DataFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val schema = Encoders.product[Project].schema

    val projectDs: Dataset[Project] = streamValue.select(from_json($"value", schema) as "parsed")
      .select("parsed.*")
      .as[Project]

    projectDs.printSchema

    writeToElastic(projectDs, "projectspark");

    val query: StreamingQuery = projectDs.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query
  }
}
