package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object ByteDecoderNext {
  val decode: String => Byte :+: ColumnError.Error :+: CNil =
    value => Try(value.toByte).fold(
      error => Coproduct[Byte :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Byte :+: ColumnError.Error :+: CNil](_)
    )
}