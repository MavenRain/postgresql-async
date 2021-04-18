package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object IntegerEncoderDecoderNext {
  val encode: Int => String = _.toString
  val decode: String => Int :+: ColumnError.Error :+: CNil =
    value => Try(value.toInt).fold(
      error => Coproduct[Int :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Int :+: ColumnError.Error :+: CNil](_)
    )
}