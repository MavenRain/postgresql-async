package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object BigIntegerEncoderDecoderNext {
  val encode: BigInt => String = _.toString
  val decode: String => BigInt :+: ColumnError.Error :+: CNil =
    value => Try(BigInt(value)).fold(
      error => Coproduct[BigInt :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[BigInt :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): BigInt :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}