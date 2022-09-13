package com.grey

import com.grey.configurations.{DataConfiguration, DataSchema}
import com.grey.environment.LocalSettings
import com.grey.estimates.CumulativeDepartures
import com.grey.functions.ScalaCaseClass
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths


/**
 *
 * @param spark : An instance of SparkSession
 */
class Algorithms(spark: SparkSession) {

  private val dataSchema = new DataSchema(spark = spark)
  private val cumulativeDepartures = new CumulativeDepartures(spark = spark)
  private val dataConfiguration = new DataConfiguration()
  private val localSettings = new LocalSettings()

  /**
   *
   * @param dataStrings : A list of a data set's Paths.get(path, file name + file extension) strings
   */
  def algorithms(dataStrings: Array[String]): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._


    dataStrings.par.foreach { dataString =>


      // A data set
      val readings: DataFrame = spark.read.schema(dataSchema.dataSchema()).json(dataString)


      // Adding field <start_date> - the start date.
      val addStarting: DataFrame = readings.withColumn(colName = "start_date", col = $"started_at".cast(DateType))


      // Using spark Dataset[]
      val baseline: Dataset[Row] = addStarting.as(
        ScalaCaseClass.scalaCaseClass(schema = addStarting.schema)).persist(StorageLevel.MEMORY_ONLY)
      baseline.show(5)


      // Examples
      cumulativeDepartures.cumulativeDepartures(baseline = baseline)


      // Writing
      println(FilenameUtils.getBaseName(Paths.get(dataString).getParent.toString))
      println(FilenameUtils.getBaseName(dataString))


      /*
      Exception.allCatch.withTry(
        baseline.coalesce(1).write
          .option("header", value = true)
          .option("encoding", value = "UTF-8")
          .option("timestampFormat", value = dataConfiguration.sourceTimeStamp)
          .option("dateFormat", value = "yyy-MM-dd")
          .csv(path = Paths.get(localSettings.warehouseDirectory).toString)

      )
      */


    }

  }

}
