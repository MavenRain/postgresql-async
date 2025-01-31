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

package io.github.mavenrain.async.db.postgresql.messages.frontend

import io.github.mavenrain.async.db.column.ColumnEncoderRegistry
import io.github.mavenrain.async.db.postgresql.messages.backend.ServerMessage

class PreparedStatementOpeningMessage(statementId: Int, query: String, values: Seq[Any], encoderRegistry : ColumnEncoderRegistry)
  extends PreparedStatementMessage(statementId: Int, ServerMessage.Parse, query, values, encoderRegistry) {

  override def toString() : String =
    s"${getClass.getSimpleName}(id=${statementId},query=${query},values=${values}})"

}