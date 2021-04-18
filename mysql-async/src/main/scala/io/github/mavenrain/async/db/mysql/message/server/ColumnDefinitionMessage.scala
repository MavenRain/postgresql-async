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

package io.github.mavenrain.async.db.mysql.message.server

import io.github.mavenrain.async.db.mysql.column.ColumnTypes
import io.github.mavenrain.async.db.mysql.util.CharsetMapper
import io.github.mavenrain.async.db.general.ColumnData
import io.github.mavenrain.async.db.mysql.binary.decoder.BinaryDecoder
import io.github.mavenrain.async.db.column.ColumnDecoder

case class ColumnDefinitionMessage(
                                    catalog: String,
                                    schema: String,
                                    table: String,
                                    originalTable: String,
                                    name: String,
                                    originalName: String,
                                    characterSet: Int,
                                    columnLength: Long,
                                    columnType: Int,
                                    flags: Short,
                                    decimals: Byte,
                                    binaryDecoder: BinaryDecoder,
                                    textDecoder: ColumnDecoder
                                    )
  extends ServerMessage(ServerMessage.ColumnDefinition)
  with ColumnData {

  def dataType: Int = columnType
  def dataTypeSize : Long = columnLength

  override def toString: String = {
    val columnTypeName = ColumnTypes.Mapping.getOrElse(columnType, columnType)
    val charsetName = CharsetMapper.DefaultCharsetsById.getOrElse(characterSet, characterSet)

    s"${getClass.getSimpleName}(name=$name,columnType=${columnTypeName},table=$table,charset=$charsetName,decimals=$decimals})"
  }
}
