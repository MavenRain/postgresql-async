package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset

object TimeWithTimezoneEncodoerDecoderNext {
  //private val format = DateTimeFormat.forPattern("HH:mm:ss.SSSSSSZ")
  val encode = TimeEncoderDecoderNext.encode
  val decode = TimeEncoderDecoderNext.decode
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset) =
    TimeEncoderDecoderNext.decode(kind, value, charset)
}