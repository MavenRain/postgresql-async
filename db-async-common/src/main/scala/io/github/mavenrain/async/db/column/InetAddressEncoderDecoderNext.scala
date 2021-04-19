package io.github.mavenrain.async.db.column

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.general.ColumnData
import java.net.InetAddress
import java.nio.charset.Charset
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import shapeless.{:+:, CNil, Coproduct}
import sun.net.util.IPAddressUtil.{textToNumericFormatV4,textToNumericFormatV6}

object InetAddressEncoderDecoderNext {
  val encode: InetAddress => String = _.getHostAddress
  val decode: String => InetAddress :+: ColumnError.Error :+: CNil =
    value => Try(InetAddress.getByAddress(
      if (value contains ':') textToNumericFormatV6(value)
      else textToNumericFormatV4(value)
    )).fold(
      error => Coproduct[InetAddress :+: ColumnError.Error :+: CNil](ColumnError(error.getMessage)),
      Coproduct[InetAddress :+: ColumnError.Error :+: CNil](_)
    )
  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): InetAddress :+: ColumnError.Error :+: CNil =
    new Array[Byte](value.readableBytes())
      .tap(value.readBytes(_))
      .pipe(bytes => decode(new String(bytes, charset)))
}