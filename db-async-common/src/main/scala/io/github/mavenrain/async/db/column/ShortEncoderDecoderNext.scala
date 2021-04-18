package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object ShortEncoderDecoderNext {
  val encode: Short => String = _.toString
  val decode: String => Short :+: ColumnError.Error :+: CNil =
    value => Try(value.toShort).fold(
      error => Coproduct[Short :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Short :+: ColumnError.Error :+: CNil](_)
    )
}