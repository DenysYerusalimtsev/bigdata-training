package com.dyerus.bigdata.sparksql

import com.dyerus.bigdata.sparksql.tasks.ProjectMember
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object SparkSqlApplication extends App with Spark {
  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "project_member_added")
    .load()

  val streamValue: DataFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  val schema = Encoders.product[ProjectMember].schema

  val projectMemberDF: Dataset[ProjectMember] = streamValue.select(from_json($"value", schema) as "parsed")
    .select("parsed.*")
    .as[ProjectMember]

  val query = projectMemberDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()
}
