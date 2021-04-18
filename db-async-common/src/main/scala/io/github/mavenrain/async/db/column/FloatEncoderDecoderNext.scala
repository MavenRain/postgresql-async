package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object FloatEncoderDecoderNext {
  val encode: Float => String = _.toString
  val decode: String => Float :+: ColumnError.Error :+: CNil =
    value => Try(value.toFloat).fold(
      error => Coproduct[Float :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Float :+: ColumnError.Error :+: CNil](_)
    )
}