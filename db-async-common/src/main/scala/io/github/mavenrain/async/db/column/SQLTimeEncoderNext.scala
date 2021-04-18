package io.github.mavenrain.async.db.column

import java.sql.Time
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.LocalTime

object SQLTimeEncoderNext {
  final private val format =
    new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .toFormatter
  val encode: Time => String =
    time => format.print(new LocalTime(time.getTime))
}