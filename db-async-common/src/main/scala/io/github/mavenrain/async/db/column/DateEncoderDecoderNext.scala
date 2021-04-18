package io.github.mavenrain.async.db.column

import java.sql.Date
import org.joda.time.{ReadablePartial, LocalDate}
import org.joda.time.format.DateTimeFormat.forPattern
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct, Poly1}

object DateEncoderDecoderNext {
  private val formatter = forPattern("yyyy-MM-dd")
  object Encoder extends Poly1 {
    implicit def date = at[Date] { new LocalDate(_).pipe(formatter.print(_)) }
    implicit def partial = at[ReadablePartial] { formatter.print(_) }
  }
  val decode: String => LocalDate :+: ColumnError.Error :+: CNil =
    value => Try(formatter.parseLocalDate(value)).fold(
      error => Coproduct[LocalDate :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[LocalDate :+: ColumnError.Error :+: CNil](_)
    )
}