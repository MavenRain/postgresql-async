package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import java.util.UUID
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}

object UUIDEncoderDecoderNext {
  val encode: UUID => String = _.toString
  val decode: String => UUID :+: ColumnError.Error :+: CNil =
    value => Try(UUID.fromString(value)).fold(
      error => Coproduct[UUID :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[UUID :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): UUID :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}