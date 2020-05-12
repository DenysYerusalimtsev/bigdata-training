package com.dyerus.bigdata.sparksql.tasks

import com.dyerus.bigdata.sparksql.{Elasticsearch, Spark}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery

object WriteProjectMemberToElastic extends Elasticsearch with Spark {
  def run()(implicit spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "project_members_added")
      .load()

    val streamValue: DataFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val schema = Encoders.product[ProjectMember].schema

    val projectMemberDs: Dataset[ProjectMember] = streamValue.select(from_json($"value", schema) as "parsed")
      .select("parsed.*")
      .as[ProjectMember]

    writeToElastic(projectMemberDs, "projectmembers");

    val query: StreamingQuery = projectMemberDs.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query
  }
}
