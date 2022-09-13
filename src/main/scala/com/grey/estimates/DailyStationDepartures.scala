package com.grey.estimates

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.count


class DailyStationDepartures(spark: SparkSession) {

  def dailyStationDepartures(baseline: Dataset[Row]): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._

    baseline.groupBy($"start_station_id", $"start_date")
      .agg(count("*").as("daily_station_departures"))
      .orderBy($"start_station_id".asc_nulls_last, $"start_date".asc_nulls_last)
      .show(5)

    baseline.select($"start_station_id", $"start_date")
      .rollup($"start_station_id", $"start_date")
      .agg(count("*").as("daily_station_departures"))
      .orderBy($"start_station_id".asc_nulls_last, $"start_date".asc_nulls_last)
      .show(5)

  }

}
