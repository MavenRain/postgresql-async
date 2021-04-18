package io.github.mavenrain.async.db.column

import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object BigDecimalEncoderDecoderNext {
  val encode: BigDecimal => String = _.toString
  val decode: String => BigDecimal :+: ColumnError.Error :+: CNil =
    value => Try(BigDecimal(value)).fold(
      error => Coproduct[BigDecimal :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[BigDecimal :+: ColumnError.Error :+: CNil](_)
    )
}