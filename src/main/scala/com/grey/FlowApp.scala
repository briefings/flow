package com.grey

import com.grey.configurations.DataConfiguration
import com.grey.environment.{DataDirectories, LocalSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try

object FlowApp {

  private val localSettings = new LocalSettings()
  private val dataDirectories = new DataDirectories()

  def main(args: Array[String]): Unit = {


    // Minimising log information output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("aka").setLevel(Level.OFF)


    // Spark Session
    val spark: SparkSession = SparkSession.builder().appName("rides")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", localSettings.warehouseDirectory)
      .getOrCreate()


    // Directories
    List(localSettings.dataDirectory, localSettings.warehouseDirectory).par.map(
      directory => dataDirectories.localDirectoryReset(directory)
    )

    val dataConfiguration = new DataConfiguration()

  }

}
