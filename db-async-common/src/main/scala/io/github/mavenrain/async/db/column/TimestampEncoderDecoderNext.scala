package io.github.mavenrain.async.db.column

import java.sql.Timestamp
import java.util.{Calendar, Date}
import org.joda.time.{DateTime, LocalDateTime, ReadableDateTime}
import org.joda.time.format.DateTimeFormatterBuilder
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct, Poly1}

object TimestampEncoderDecoderNext {
  private val BaseFormat = "yyyy-MM-dd HH:mm:ss"
  private val MillisFormat = ".SSSSSS"
  private val Instance = new TimestampEncoderDecoder()
  private val optional = new DateTimeFormatterBuilder()
    .appendPattern(MillisFormat).toParser
  private val optionalTimeZone = new DateTimeFormatterBuilder()
    .appendPattern("Z").toParser
  private val builder = new DateTimeFormatterBuilder()
    .appendPattern(BaseFormat)
    .appendOptional(optional)
    .appendOptional(optionalTimeZone)
  private val timezonedPrinter = new DateTimeFormatterBuilder()
    .appendPattern(s"${BaseFormat}${MillisFormat}Z").toFormatter
  private val nonTimezonedPrinter = new DateTimeFormatterBuilder()
    .appendPattern(s"${BaseFormat}${MillisFormat}").toFormatter
  private val format = builder.toFormatter
  object Encode extends Poly1 {
    implicit def timestamp = at[Timestamp] { new DateTime(_).pipe(timezonedPrinter.print(_)) }
    implicit def date = at[Date] { new DateTime(_).pipe(timezonedPrinter.print(_)) }
    implicit def calendar = at[Calendar] { new DateTime(_).pipe(timezonedPrinter.print(_)) }
    implicit def localDateTime = at[LocalDateTime] { nonTimezonedPrinter.print(_) }
    implicit def readableDateTime = at[ReadableDateTime] { timezonedPrinter.print(_) }
  }
  val decode: String => LocalDateTime :+: ColumnError.Error :+: CNil =
    value => Try(format.parseLocalDateTime(value)).fold(
      error => Coproduct[LocalDateTime :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[LocalDateTime :+: ColumnError.Error :+: CNil](_)
    )
}