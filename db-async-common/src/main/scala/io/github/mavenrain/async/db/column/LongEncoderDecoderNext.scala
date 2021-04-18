package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object LongEncoderDecoderNext {
  val encode: Long => String = _.toString
  val decode: String => Long :+: ColumnError.Error :+: CNil =
    value => Try(value.toLong).fold(
      error => Coproduct[Long :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[Long :+: ColumnError.Error :+: CNil](_)
    )
}