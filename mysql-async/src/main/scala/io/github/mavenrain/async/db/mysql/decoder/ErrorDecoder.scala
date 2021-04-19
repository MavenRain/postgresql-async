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
import io.github.mavenrain.async.db.mysql.message.server.{ErrorMessage, ServerMessage}
import io.github.mavenrain.async.db.util.ChannelWrapper.bufferToWrapper
import java.nio.charset.Charset

class ErrorDecoder( charset : Charset ) extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {

    new ErrorMessage(
      buffer.readShort(),
      buffer.readFixedString( 6, charset ),
      buffer.readUntilEOF(charset)
    )

  }

}
