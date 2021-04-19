package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object TimestampWithTimezoneEncoderDecoderNext {
  private val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ")
  val encode = TimestampEncoderDecoderNext.Encode
  val decode: String => DateTime :+: ColumnError.Error :+: CNil =
    value => Try(format.parseDateTime(value)).fold(
      error => Coproduct[DateTime :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[DateTime :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): DateTime :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}