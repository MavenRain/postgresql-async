package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.nio.charset.Charset
import scala.util.chaining.scalaUtilChainingOps

object StringEncoderDecoderNext {
  val encode: String => String = identity
  val decode: String => String = encode
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): String =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => new String(bytes, charset))
}