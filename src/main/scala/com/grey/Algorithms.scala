package com.grey

import com.grey.configurations.DataSchema
import com.grey.functions.ScalaCaseClass
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.storage.StorageLevel


/**
 *
 * @param spark: An instance of SparkSession
 */
class Algorithms(spark: SparkSession) {

  private val dataSchema = new DataSchema(spark = spark)

  /**
   *
   * @param dataStrings: A list of a data set's Paths.get(path, file name + file extension) strings
   */
  def algorithms(dataStrings: Array[String]): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._


    // A window calculation specification
    val windowSpec: WindowSpec = Window.partitionBy($"start_station_id")
      .orderBy($"start_date".asc_nulls_last)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    dataStrings.par.foreach { dataString =>


      // A data set
      val readings: DataFrame = spark.read.schema(dataSchema.dataSchema()).json(dataString)


      // Adding field <start_date> - the start date.
      val addStarting: DataFrame = readings.withColumn(colName = "start_date", col = $"started_at".cast(DateType))


      // Using spark Dataset[]
      val baseline: Dataset[Row] = addStarting.as(
        ScalaCaseClass.scalaCaseClass(schema = addStarting.schema)).persist(StorageLevel.MEMORY_ONLY)


      // Examples
      baseline.groupBy($"start_date")
        .agg(count("*").as("daily_departures"))
        .orderBy($"start_date".asc_nulls_last)
        .show(5)

      baseline.groupBy($"start_station_id", $"start_date")
        .agg(count("*").as("daily_station_departures"))
        .orderBy($"start_station_id".asc_nulls_last, $"start_date".asc_nulls_last)
        .show(5)

      baseline.select($"start_station_id", $"start_date")
        .rollup($"start_station_id", $"start_date")
        .agg(count("*").as("daily_station_departures"))
        .orderBy($"start_station_id".asc_nulls_last, $"start_date".asc_nulls_last)
        .show(5)

      baseline.groupBy($"start_station_id", $"start_date")
        .agg(count("*").as("outward"))
        .select($"start_station_id", $"start_date",
          sum($"outward").over(windowSpec).as("daily_station_departures_c"))
        .show(5)

    }

  }

}
