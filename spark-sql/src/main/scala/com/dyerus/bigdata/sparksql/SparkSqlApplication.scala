package com.dyerus.bigdata.sparksql

import com.dyerus.bigdata.sparksql.tasks.{WriteProjectMemberToElastic}

object SparkSqlApplication extends App with Spark {
  val writeToElasticQuery = WriteProjectMemberToElastic.run()

  writeToElasticQuery.awaitTermination()
}
