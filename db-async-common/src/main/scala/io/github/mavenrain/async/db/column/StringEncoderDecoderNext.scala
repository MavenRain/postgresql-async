package io.github.mavenrain.async.db.column

object StringEncoderDecoderNext {
  val encode: String => String = identity
  val decode: String => String = encode
}