package com.dyerus.bigdata.sparksql.tasks

case class Project (projectIdentifier: String,
                    programme: String,
                    keyAction: String,
                    actionType: String,
                    callYear: Long,
                    projectTitle:String,
                    topics: String,
                    projectSummary: String,
                    projectStatus: String,
                    euGrantAward: Option[Double],
                    isGoodPractice: Option[Boolean],
                    isSuccessStory: Option[Boolean],
                    projectWebsite: String,
                    resultsAvailable: Option[Boolean],
                    resultsPlatformProjectCard: String,
                    participatingCountries: Array[String],
                    organisationInfo: OrganisationInfo)