package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object FloatEncoderDecoderNext {
  val encode: Float => String = _.toString
  val decode: String => Float :+: ColumnError.Error :+: CNil =
    value => Try(value.toFloat).fold(
      error => Coproduct[Float :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Float :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Float :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}