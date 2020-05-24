package com.dyerus.bigdata.sparksql

import com.dyerus.bigdata.sparksql.tasks.{PrintProjectsWithYoungestEmployee, ProjectWithMostPeople, WriteProjectMemberToElastic, WriteProjectToElastic}

object SparkSqlApplication extends App with Spark {
  /*val writeToElasticQuery = WriteProjectMemberToElastic.run()
  writeToElasticQuery.awaitTermination()*/

  /*val writeToElasticQuery = WriteProjectToElastic.run()
  writeToElasticQuery.awaitTermination()*/

  /*val printProjectWithYoungestEmployee = PrintProjectsWithYoungestEmployee.run()
  printProjectWithYoungestEmployee.awaitTermination()*/
  val projectWithMostPeople = ProjectWithMostPeople.run
  projectWithMostPeople.awaitTermination
}
