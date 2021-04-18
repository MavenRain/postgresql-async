package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object DoubleEncoderDecoderNext {
  val encode: Double => String = _.toString
  val decode: String => Double :+: ColumnError.Error :+: CNil =
    value => Try(value.toDouble).fold(
      error => Coproduct[Double :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Double :+: ColumnError.Error :+: CNil](_)
    )
}