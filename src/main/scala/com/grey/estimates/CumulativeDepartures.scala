package com.grey.estimates

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.grey.functions.ScalaCaseClass


/**
 *
 * @param spark: An instance of SparkSession
 */
class CumulativeDepartures(spark: SparkSession) {


  /**
   * Import implicits for
   * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
   * implicit conversions, e.g., converting a RDD to a DataFrames.
   * access to the "$" notation.
   */
  import spark.implicits._


  // A window calculation specification
  private val windowSpec: WindowSpec = Window.partitionBy($"start_station_id")
    .orderBy($"start_station_id".asc_nulls_last, $"start_date".asc_nulls_last)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


  /**
   *
   * @param baseline: Data set
   */
  def cumulativeDepartures(baseline: Dataset[Row]): Dataset[Row] = {

    val data: DataFrame = baseline.groupBy($"start_station_id", $"start_date")
      .agg(count("*").as("outward"))
      .select($"start_station_id", $"start_date",
        sum($"outward").over(windowSpec).as("daily_station_departures_c"))

    data.as(ScalaCaseClass.scalaCaseClass(schema = data.schema))

  }

}
