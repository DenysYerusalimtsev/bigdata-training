package com.dyerus.bigdata.sparksql.tasks

import java.sql.Timestamp

import com.dyerus.bigdata.sparksql.{Elasticsearch, Spark}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object PrintProjectsWithYoungestEmployee extends Elasticsearch with Spark {
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
        .groupBy(
          //window($"timestamp", "1 seconds", "1 seconds"),
          $"projectId")
 //       .as[ProjectMember]

//    val projects: Dataset[Project] = readFromElastic("projectspark")
//
//    val projectMembers: Dataset[ProjectMember] = readFromElastic("projectmembers")
//
//    val joinedProjectMembers: Dataset[ProjectMember] = projectMemberDs //.union(projectMembers)
//

    val groupedWithYoungest: DataFrame = projectMemberDs.agg(max("memberYearOfBirth"))

//    val joined: Dataset[(Project, ProjectMember)] = projects.joinWith(
//      projectMemberDs,
//      projectMemberDs("projectId") === projects("projectIdentifier"))


    val query: StreamingQuery = groupedWithYoungest.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query
  }
}
