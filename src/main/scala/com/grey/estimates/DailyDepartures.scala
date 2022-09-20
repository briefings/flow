package com.grey.estimates

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.count


class DailyDepartures(spark: SparkSession) {

  def dailyDepartures(baseline: Dataset[Row]): Dataset[Row] = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._

    baseline.groupBy($"start_date")
      .agg(count("*").as("daily_departures"))
      .orderBy($"start_date".asc_nulls_last)

  }

}
