package io.github.mavenrain.async.db.column

import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormatterBuilder
import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object TimeEncoderDecoderNext {
  final private val optional = new DateTimeFormatterBuilder()
    .appendPattern(".SSSSSS").toParser

  final private val format = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendOptional(optional)
    .toFormatter

  final private val printer = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss.SSSSSS")
    .toFormatter
  val encode: LocalTime => String = printer.print(_)
  val decode: String => LocalTime :+: ColumnError.Error :+: CNil =
    value => Try(format.parseLocalTime(value)).fold(
      error => Coproduct[LocalTime :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[LocalTime :+: ColumnError.Error :+: CNil](_)
    )
}