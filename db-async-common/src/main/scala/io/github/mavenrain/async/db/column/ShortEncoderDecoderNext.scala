package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object ShortEncoderDecoderNext {
  val encode: Short => String = _.toString
  val decode: String => Short :+: ColumnError.Error :+: CNil =
    value => Try(value.toShort).fold(
      error => Coproduct[Short :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Short :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Short :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}