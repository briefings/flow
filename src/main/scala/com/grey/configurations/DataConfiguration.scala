package com.grey.configurations

import com.grey.environment.LocalSettings
import com.grey.functions.{TimeBoundaries, TimeFormats, TimeSeries}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime

import java.io.File
import java.nio.file.Paths
import scala.util.Try
import scala.util.control.Exception


class DataConfiguration {

  /**
   * Path
   */
  private val localSettings = new LocalSettings()
  private val path: String = Paths.get(localSettings.resourcesDirectory, "data.conf").toString


  /**
   * Read the configuration file
   */
  private val config: Try[Config] = Exception.allCatch.withTry(
    ConfigFactory.parseFile(new File(path)).getConfig("variables")
  )
  if (config.isFailure) {
    sys.error(config.failed.get.getMessage)
  }


  /**
   * Query formula
   */
  private val variable: (String, String) => String = (group: String, variable: String) => {
    val text = Exception.allCatch.withTry(
      config.get.getConfig(group).getString(variable)
    )
    if (text.isFailure) {
      sys.error(text.failed.get.getMessage)
    } else {
      text.get
    }
  }


  /**
   * The date boundaries of interest; is <from> prior to <until>?
   */
  private val timeFormats = new TimeFormats(
    dateTimePattern = variable("url", "dateTimePattern"))
  private val from: DateTime = timeFormats.timeFormats(dateString = variable("times", "endDate"))
  private val until: DateTime = timeFormats.timeFormats(dateString = variable("times", "startDate"))
  new TimeBoundaries().timeBoundaries(from = from, until = until)


  /**
   * The list of dates
   */
  private val T: Try[List[DateTime]] = Exception.allCatch.withTry(
    new TimeSeries().timeSeries(from = from, until = until,
      step = variable("times", "step").toInt, stepType = variable("times", "stepType"))
  )
  val listOfDates: List[DateTime] =  if (T.isSuccess) {
    T.get
  } else {
    sys.error(T.failed.get.getMessage)
  }


  /**
   * Additionally
   */
  val api: String = variable("url", "api")
  val sourceTimeStamp: String = variable("times", "sourceTimeStamp")
  val dateTimePattern: String = variable("url", "dateTimePattern")

}
