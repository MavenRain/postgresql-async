package io.github.mavenrain.async.db.column

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object TimestampWithTimezoneEncoderDecoderNext {
  private val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ")
  val encode = TimestampEncoderDecoderNext.Encode
  val decode: String => DateTime :+: ColumnError.Error :+: CNil =
    value => Try(format.parseDateTime(value)).fold(
      error => Coproduct[DateTime :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[DateTime :+: ColumnError.Error :+: CNil](_)
    )
}