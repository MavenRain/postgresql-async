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

package io.github.mavenrain.async.db.postgresql.column

import io.github.mavenrain.async.db.column.ColumnEncoderDecoder

object SingleByteEncoderDecoder extends ColumnEncoderDecoder {

  override def encode(value: Any): String = {
    val byte = value.asInstanceOf[Byte]
    ByteArrayEncoderDecoder.encode(Array(byte))
  }

  override def decode(value: String): Any = {
    ByteArrayEncoderDecoder.decode(value)(0)
  }

}
