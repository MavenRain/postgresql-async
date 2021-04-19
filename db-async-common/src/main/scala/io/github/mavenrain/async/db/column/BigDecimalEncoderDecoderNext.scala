package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object BigDecimalEncoderDecoderNext {
  val encode: BigDecimal => String = _.toString
  val decode: String => BigDecimal :+: ColumnError.Error :+: CNil =
    value => Try(BigDecimal(value)).fold(
      error => Coproduct[BigDecimal :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[BigDecimal :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): BigDecimal :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}