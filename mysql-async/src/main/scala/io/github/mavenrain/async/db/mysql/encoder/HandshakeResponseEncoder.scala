/*
 * Copyright 2021 Onyekachukwu Obi
 *
 * Onyekachukwu Obi licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.mavenrain.async.db.mysql.encoder

import java.nio.charset.Charset

import io.github.mavenrain.async.db.exceptions.UnsupportedAuthenticationMethodException
import io.github.mavenrain.async.db.mysql.encoder.auth.AuthenticationMethod
import io.github.mavenrain.async.db.mysql.message.client.{ClientMessage, HandshakeResponseMessage}
import io.github.mavenrain.async.db.mysql.util.CharsetMapper
import io.github.mavenrain.async.db.util.{ByteBufferUtils, Log}
import io.netty.buffer.ByteBuf

object HandshakeResponseEncoder {

  final val MAX_3_BYTES = 0x00ffffff
  final val PADDING: Array[Byte] = List.fill(23) {
    0.toByte
  }.toArray

  final val log = Log.get[HandshakeResponseEncoder]

}

class HandshakeResponseEncoder(charset: Charset, charsetMapper: CharsetMapper) extends MessageEncoder {

  import io.github.mavenrain.async.db.mysql.encoder.HandshakeResponseEncoder._
  import io.github.mavenrain.async.db.mysql.util.MySQLIO._

  private val authenticationMethods = AuthenticationMethod.Availables

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[HandshakeResponseMessage]

    var clientCapabilities = 0

    clientCapabilities |=
      CLIENT_PLUGIN_AUTH |
      CLIENT_PROTOCOL_41 |
      CLIENT_TRANSACTIONS |
      CLIENT_MULTI_RESULTS |
      CLIENT_SECURE_CONNECTION

    if (m.database.isDefined) {
      clientCapabilities |= CLIENT_CONNECT_WITH_DB
    }

    val buffer = ByteBufferUtils.packetBuffer()

    buffer.writeInt(clientCapabilities)
    buffer.writeInt(MAX_3_BYTES)
    buffer.writeByte(charsetMapper.toInt(charset))
    buffer.writeBytes(PADDING)
    ByteBufferUtils.writeCString( m.username, buffer, charset )

    if ( m.password.isDefined ) {
      val method = m.authenticationMethod
      val authenticator = authenticationMethods.getOrElse(
        method, { throw new UnsupportedAuthenticationMethodException(method) })
      val bytes = authenticator.generateAuthentication(charset, m.password, m.seed)
      buffer.writeByte(bytes.length)
      buffer.writeBytes(bytes)
    } else {
      buffer.writeByte(0)
    }

    if ( m.database.isDefined ) {
      ByteBufferUtils.writeCString( m.database.get, buffer, charset )
    }

    ByteBufferUtils.writeCString( m.authenticationMethod, buffer, charset )

    buffer
  }

}
