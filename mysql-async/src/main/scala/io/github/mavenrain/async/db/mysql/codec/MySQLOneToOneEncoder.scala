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

package io.github.mavenrain.async.db.mysql.codec

import io.github.mavenrain.async.db.exceptions.EncoderNotAvailableException
import io.github.mavenrain.async.db.mysql.binary.BinaryRowEncoder
import io.github.mavenrain.async.db.mysql.encoder._
import io.github.mavenrain.async.db.mysql.message.client.ClientMessage
import io.github.mavenrain.async.db.mysql.util.CharsetMapper
import io.github.mavenrain.async.db.util.{BufferDumper, ByteBufferUtils, Log}
import java.nio.charset.Charset
import scala.annotation.switch
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.buffer.ByteBuf

object MySQLOneToOneEncoder {
  val log = Log.get[MySQLOneToOneEncoder]
}

class MySQLOneToOneEncoder(charset: Charset, charsetMapper: CharsetMapper)
    extends MessageToMessageEncoder[ClientMessage](classOf[ClientMessage]) {

  import MySQLOneToOneEncoder.log

  private final val handshakeResponseEncoder = new HandshakeResponseEncoder(charset, charsetMapper)
  private final val queryEncoder = new QueryMessageEncoder(charset)
  private final val rowEncoder = new BinaryRowEncoder(charset)
  private final val prepareEncoder = new PreparedStatementPrepareEncoder(charset)
  private final val executeEncoder = new PreparedStatementExecuteEncoder(rowEncoder)
  private final val authenticationSwitchEncoder = new AuthenticationSwitchResponseEncoder(charset)

  private var sequence = 1

  def encode(ctx: ChannelHandlerContext, message: ClientMessage, out: java.util.List[Object]): Unit = {
    val encoder = (message.kind: @switch) match {
      case ClientMessage.ClientProtocolVersion => handshakeResponseEncoder
      case ClientMessage.Quit => {
        sequence = 0
        QuitMessageEncoder
      }
      case ClientMessage.Query => {
        sequence = 0
        queryEncoder
      }
      case ClientMessage.PreparedStatementExecute => {
        sequence = 0
        executeEncoder
      }
      case ClientMessage.PreparedStatementPrepare => {
        sequence = 0
        prepareEncoder
      }
      case ClientMessage.AuthSwitchResponse => {
        sequence += 1
        authenticationSwitchEncoder
      }
      case _ => throw new EncoderNotAvailableException(message)
    }

    val result: ByteBuf = encoder.encode(message)

    ByteBufferUtils.writePacketLength(result, sequence)

    sequence += 1

    if ( log.isTraceEnabled ) {
      log.trace(s"Writing message ${message.getClass.getName} - \n${BufferDumper.dumpAsHex(result)}")
    }

    out.add(result)
  }

}
