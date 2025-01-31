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

package io.github.mavenrain.async.db.mysql.decoder

import io.netty.buffer.ByteBuf
import io.github.mavenrain.async.db.mysql.message.server.ColumnDefinitionMessage
import io.github.mavenrain.async.db.util.ChannelWrapper.bufferToWrapper
import io.github.mavenrain.async.db.util.Log
import java.nio.charset.Charset
import io.github.mavenrain.async.db.mysql.codec.DecoderRegistry

object ColumnDefinitionDecoder {
  final val log = Log.get[ColumnDefinitionDecoder]
}

class ColumnDefinitionDecoder(charset: Charset, registry : DecoderRegistry) extends MessageDecoder {

  override def decode(buffer: ByteBuf): ColumnDefinitionMessage = {

    val catalog = buffer.readLengthEncodedString(charset)
    val schema = buffer.readLengthEncodedString(charset)
    val table = buffer.readLengthEncodedString(charset)
    val originalTable = buffer.readLengthEncodedString(charset)
    val name = buffer.readLengthEncodedString(charset)
    val originalName = buffer.readLengthEncodedString(charset)

    buffer.readBinaryLength

    val characterSet = buffer.readUnsignedShort()
    val columnLength = buffer.readUnsignedInt()
    val columnType = buffer.readUnsignedByte()
    val flags = buffer.readShort()
    val decimals = buffer.readByte()

    buffer.readShort()

    new ColumnDefinitionMessage(
      catalog,
      schema,
      table,
      originalTable,
      name,
      originalName,
      characterSet,
      columnLength,
      columnType,
      flags,
      decimals,
      registry.binaryDecoderFor(columnType, characterSet),
      registry.textDecoderFor(columnType,characterSet)
    )
  }

}