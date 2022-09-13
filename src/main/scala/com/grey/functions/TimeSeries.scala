package com.grey.functions

import org.joda.time.DateTime


/**
 * Creates time series
 */
class TimeSeries {

  /**
   *
   * @param from: Starting
   * @param until: Ending
   * @param step: Granularity
   * @param stepType: the granularity's unit of measure, e.g., days, months, etc.
   * @return
   */
  def timeSeries(from: DateTime, until: DateTime, step: Int, stepType: String): List[DateTime] = {

    val timeSeriesIterator: Iterator[DateTime]  = stepType match {
      case "days" =>
        Iterator.iterate(from)( x => x.plusDays(step)).takeWhile(x => !x.isAfter(until) )
      case "months" =>
        Iterator.iterate(from)( x => x.plusMonths(step)).takeWhile(x => !x.isAfter(until) )
      case _ =>
        sys.error("Unknown Step Type")
    }

    timeSeriesIterator.toList

  }

}
