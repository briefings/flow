package com.grey

import com.grey.configurations.DataConfiguration
import com.grey.environment.{LocalDirectories, LocalSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FlowApp {

  private val localSettings = new LocalSettings()
  private val localDirectories = new LocalDirectories()

  def main(args: Array[String]): Unit = {


    // Minimising log information output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("aka").setLevel(Level.OFF)


    // Spark Session
    val spark: SparkSession = SparkSession.builder().appName("rides")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", localSettings.warehouseDirectory)
      .getOrCreate()


    // Spark logs
    spark.sparkContext.setLogLevel("ERROR")


    // Directories
    localDirectories.localDirectoryReset(directoryName = localSettings.warehouseDirectory)


    // Data
    val dataConfiguration: DataConfiguration = new DataConfiguration()
    new ImportData(api = dataConfiguration.api, dateTimePattern = dataConfiguration.dateTimePattern)
      .importData(listOfDates = dataConfiguration.listOfDates)

  }

}
