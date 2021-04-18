package io.github.mavenrain.async.db.column

import java.util.UUID
import scala.util.Try
import shapeless.{:+:, CNil, Coproduct}

object UUIDEncoderDecoderNext {
  val encode: UUID => String = _.toString
  val decode: String => UUID :+: ColumnError.Error :+: CNil =
    value => Try(UUID.fromString(value)).fold(
      error => Coproduct[UUID :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[UUID :+: ColumnError.Error :+: CNil](_)
    )
}