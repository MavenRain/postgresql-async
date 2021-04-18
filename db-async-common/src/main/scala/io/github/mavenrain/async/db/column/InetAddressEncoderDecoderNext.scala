package io.github.mavenrain.async.db.column

import java.net.InetAddress
import scala.util.Try
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
}