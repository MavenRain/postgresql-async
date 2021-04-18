package io.github.mavenrain.async.db.column

import zio.prelude.Newtype

object ColumnError {
  object Error extends Newtype[String]
  type Error = Error.Type
  def apply(error: String): Error = Error(error)
}