package com.grey

import com.grey.configurations.DataSchema
import com.grey.estimates.CumulativeDepartures
import com.grey.estimates.DailyStationDepartures
import com.grey.functions.ScalaCaseClass
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.parallel.mutable.ParArray


/**
 *
 * @param spark : An instance of SparkSession
 */
class Algorithms(spark: SparkSession) {

  private val dataSchema = new DataSchema(spark = spark)
  private val dailyStationDepartures = new DailyStationDepartures(spark = spark)
  private val cumulativeDepartures = new CumulativeDepartures(spark = spark)

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


    val T: ParArray[Dataset[Row]] = dataStrings.par.map { dataString =>


      // A data set
      val readings: DataFrame = spark.read.schema(dataSchema.dataSchema()).json(dataString)


      // Adding field <start_date> - the start date.
      val addStarting: DataFrame = readings.withColumn(colName = "start_date", col = $"started_at".cast(DateType))


      // Using spark Dataset[]
      val baseline: Dataset[Row] = addStarting.as(
        ScalaCaseClass.scalaCaseClass(schema = addStarting.schema)).persist(StorageLevel.MEMORY_ONLY)
      baseline.show(5)


      // Examples
      dailyStationDepartures.dailyStationDepartures(baseline = baseline)
      cumulativeDepartures.cumulativeDepartures(baseline = baseline)


    }

    T.reduce(_ union _).show()

  }

}
