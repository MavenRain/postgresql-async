package io.github.mavenrain.async.db.column

object TimeWithTimezoneEncodoerDecoderNext {
  //private val format = DateTimeFormat.forPattern("HH:mm:ss.SSSSSSZ")
  val encode = TimeEncoderDecoderNext.encode
  val decode = TimeEncoderDecoderNext.decode
}