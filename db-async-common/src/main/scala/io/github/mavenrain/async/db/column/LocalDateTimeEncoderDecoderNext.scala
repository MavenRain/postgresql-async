package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormatterBuilder
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object LocalDateTimeEncoderDecoderNext {
  private val optional =
    new DateTimeFormatterBuilder()
      .appendPattern(".SSSSSS")
      .toParser
  private val format =
    new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .appendOptional(optional)
      .toFormatter
  val encode: LocalDateTime => String = format.print(_)
  val decode: String => LocalDateTime :+: ColumnError.Error :+: CNil =
    value => Try(format.parseLocalDateTime(value)).fold(
      error => Coproduct[LocalDateTime :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[LocalDateTime :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): LocalDateTime :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}