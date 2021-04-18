package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object BigIntegerEncoderDecoderNext {
  val encode: BigInt => String = _.toString
  val decode: String => BigInt :+: ColumnError.Error :+: CNil =
    value => Try(BigInt(value)).fold(
      error => Coproduct[BigInt :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[BigInt :+: ColumnError.Error :+: CNil](_)
    )
}