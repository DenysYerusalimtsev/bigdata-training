package com.dyerus.bigdata.sparksql.tasks

import com.dyerus.bigdata.sparksql.{Elasticsearch, Spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object ProjectWithMostPeople extends Elasticsearch with Spark {

  def run()(implicit spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "project_members_added")
      .load()

    val streamValue: DataFrame = df.selectExpr(
      "CAST(key AS STRING)",
      "CAST(value AS STRING)",
      "timestamp")

    val schema = Encoders.product[ProjectMember].schema

    val projectMemberDs =
      streamValue
        .select(from_json($"value", schema) as "parsed", $"timestamp")
        .select("parsed.*", "timestamp")
        .withWatermark("timestamp", "10 seconds")
        .as[ProjectMember]

    val projects: Dataset[Project] = readFromElastic("projectspark")

    val joined: Dataset[ProjectMember] = projectMemberDs.joinWith(projects,
      projectMemberDs("projectId") === projects("projectIdentifier"))
      .map { case (m, _) => m }

    val grouped: Dataset[ProjectWithCount] = joined
      .groupByKey(_.projectId)
      .count()
      .map { case (id, count) => ProjectWithCount(id, count) }

    //val projectWithMostEmployees = grouped.agg(max("count"))

    val query: StreamingQuery = grouped.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query
  }
}

private case class ProjectWithCount(id: String, count: Long)