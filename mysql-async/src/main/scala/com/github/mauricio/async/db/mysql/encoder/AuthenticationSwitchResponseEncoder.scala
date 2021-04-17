package io.github.mavenrain.async.db.mysql.encoder

import io.github.mavenrain.async.db.mysql.message.client.{AuthenticationSwitchResponse, ClientMessage}
import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.exceptions.UnsupportedAuthenticationMethodException
import io.github.mavenrain.async.db.mysql.encoder.auth.AuthenticationMethod
import java.nio.charset.Charset
import io.github.mavenrain.async.db.util.ByteBufferUtils

class AuthenticationSwitchResponseEncoder( charset : Charset ) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val switch = message.asInstanceOf[AuthenticationSwitchResponse]

    val method = switch.request.method
    val authenticator = AuthenticationMethod.Availables.getOrElse(
    method, { throw new UnsupportedAuthenticationMethodException(method) })

    val buffer = ByteBufferUtils.packetBuffer()

    val bytes = authenticator.generateAuthentication(charset, switch.password, switch.request.seed.getBytes(charset) )
    buffer.writeBytes(bytes)

    buffer
  }

}
