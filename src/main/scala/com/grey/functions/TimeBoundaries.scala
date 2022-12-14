package com.grey.functions

import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


/**
 * Validates the time boundaries for a time series
 */
class TimeBoundaries {

  /**
   *
   * @param from: Starting
   * @param until: Ending
   * @return
   */
  def timeBoundaries(from: DateTime, until: DateTime): Boolean = {

    // Set-up from & until comparison
    val sequential: Try[Boolean] = Exception.allCatch.withTry(
      from.isBefore(until) || from.isEqual(until)
    )

    // If comparable, ascertain 'from' precedes 'until'
    if (sequential.isSuccess) {
      if (sequential.get) sequential.get else sys.error("The start date must precede the end date")
    } else {
      sys.error(sequential.failed.get.getMessage)
    }

  }

}
