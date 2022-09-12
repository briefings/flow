package com.grey

import com.grey.environment.LocalSettings
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

class Algorithms(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def algorithms(dataStrings: Array[String]): Unit = {

    dataStrings.par.foreach{dataString =>

      val readings: DataFrame = spark.read.json(dataString)
      readings.show(numRows = 5)

    }


  }

}
