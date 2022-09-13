package com.grey.configurations

import com.grey.environment.LocalSettings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import java.nio.file.Paths
import scala.util.Try
import scala.util.control.Exception

class DataSchema(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def dataSchema(): StructType = {

    // A data reading schema for the data set in question
    val schemaProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(
        path = Paths.get(localSettings.resourcesDirectory, "schema.json").toString)
    )

    // The StructType form of the schema
    val schema: StructType = if (schemaProperties.isSuccess) {
      DataType.fromJson(schemaProperties.get.collect.mkString("")).asInstanceOf[StructType]
    } else {
      sys.error(schemaProperties.failed.get.getMessage)
    }

    schema

  }

}
