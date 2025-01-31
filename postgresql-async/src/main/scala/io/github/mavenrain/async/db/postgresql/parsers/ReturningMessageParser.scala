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

package io.github.mavenrain.async.db.postgresql.parsers

import io.github.mavenrain.async.db.postgresql.messages.backend._
import io.netty.buffer.ByteBuf

object ReturningMessageParser {

  val BindCompleteMessageParser = new ReturningMessageParser(BindComplete)
  val CloseCompleteMessageParser = new ReturningMessageParser(CloseComplete)
  val EmptyQueryStringMessageParser = new ReturningMessageParser(EmptyQueryString)
  val NoDataMessageParser = new ReturningMessageParser(NoData)
  val ParseCompleteMessageParser = new ReturningMessageParser(ParseComplete)

}

class ReturningMessageParser(val message: ServerMessage) extends MessageParser {

  def parseMessage(buffer: ByteBuf): ServerMessage = message

}
