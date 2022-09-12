package com.grey

import com.grey.environment.{LocalDirectories, LocalSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.Paths

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


    // Try
    val directoryObject: File = new File(localSettings.dataDirectory)
    val directories: Array[File] = directoryObject.listFiles.filter(_.isDirectory)
    val dataStrings: Array[String] = directories.map(dir =>
      dir.listFiles.filter(_.isFile).map(_.getPath)).reduce(_ union _)


    // Data
    new Algorithms(spark = spark).algorithms(dataStrings = dataStrings)

  }

}
