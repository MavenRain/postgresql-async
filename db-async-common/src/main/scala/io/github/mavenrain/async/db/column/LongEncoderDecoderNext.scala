package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object LongEncoderDecoderNext {
  val encode: Long => String = _.toString
  val decode: String => Long :+: ColumnError.Error :+: CNil =
    value => Try(value.toLong).fold(
      error => Coproduct[Long :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Long :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Long :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}