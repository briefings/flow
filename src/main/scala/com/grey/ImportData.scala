package com.grey

import com.grey.environment.{LocalDirectories, LocalSettings}
import com.grey.functions.IsExistURL
import org.joda.time.DateTime

import java.io.File
import java.net.URL
import java.nio.file.Paths
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try
import scala.util.control.Exception


/**
 *
 * @param api: The API string of the data source
 * @param dateTimePattern: The API string's data & time pattern
 */
class ImportData(api: String, dateTimePattern: String) {

  private val localSettings = new LocalSettings()
  private val localDirectories = new LocalDirectories()
  private val isExistURL = new IsExistURL()

  /**
   *
   * @param listOfDates: A list of dates
   */
  def importData(listOfDates: List[DateTime]): Unit = {


    listOfDates.par.foreach { date =>

      // The directory into which the data of the date in question should be deposited (directoryName) and
      // the name to assign to the data file (fileString).  Note that fileString includes the path name.
      val directory: String = Paths.get(localSettings.dataDirectory, date.toString("yyyy")).toString
      val dataString = Paths.get(directory, date.toString("MM") + ".json").toString


      // The URL
      val url = api.format(date.toString(dateTimePattern))


      // Is the URL alive?
      val isURL: Try[Boolean] = isExistURL.isExistURL(url)


      // If yes, import the data set, otherwise ...
      val data: Try[String] = if (isURL.isSuccess) {
        localDirectories.localDirectoryCreate(directoryName = directory)
        Exception.allCatch.withTry(
          new URL(url) #> new File(dataString) !!
        )
      } else {
        sys.error(isURL.failed.get.getMessage)
      }


      // Finally
      if (data.isSuccess) {
        println("Successfully imported " + url)
        println(data.get)
      } else {
        sys.error(data.failed.get.getMessage)
      }

    }

  }

}
