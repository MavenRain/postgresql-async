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
import io.github.mavenrain.async.db.exceptions.DateEncoderNotAvailableException
import io.github.mavenrain.async.db.general.ColumnData
import io.github.mavenrain.async.db.postgresql.messages.backend.PostgreSQLColumnData
import io.netty.buffer.ByteBuf
import java.nio.charset.Charset
import java.sql.Timestamp
import java.util.{Calendar, Date}
import org.joda.time._
import org.joda.time.format.DateTimeFormatterBuilder

object PostgreSQLTimestampEncoderDecoder extends ColumnEncoderDecoder {

  private val optionalTimeZone = new DateTimeFormatterBuilder()
    .appendPattern("Z").toParser

  private val internalFormatters = 1.until(6).inclusive.map {
    index =>
      new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendPattern("." + ("S" * index ))
        .appendOptional(optionalTimeZone)
        .toFormatter
  }

  private val internalFormatterWithoutSeconds = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .appendOptional(optionalTimeZone)
    .toFormatter

  def formatter = internalFormatters(5)

  override def decode( kind : ColumnData, value : ByteBuf, charset : Charset ) : Any = {
    val bytes = new Array[Byte](value.readableBytes())
    value.readBytes(bytes)

    val text = new String(bytes, charset)

    val columnType = kind.asInstanceOf[PostgreSQLColumnData]

    columnType.dataType match {
      case ColumnTypes.Timestamp | ColumnTypes.TimestampArray => {
        selectFormatter(text).parseLocalDateTime(text)
      }
      case ColumnTypes.TimestampWithTimezoneArray => {
        selectFormatter(text).parseDateTime(text)
      }
      case ColumnTypes.TimestampWithTimezone => {
        if ( columnType.dataTypeModifier > 0 ) {
          internalFormatters(columnType.dataTypeModifier - 1).parseDateTime(text)
        } else {
          selectFormatter(text).parseDateTime(text)
        }
      }
    }
  }

  private def selectFormatter( text : String ) = {
    if ( text.contains(".") ) {
      internalFormatters(5)
    } else {
      internalFormatterWithoutSeconds
    }
  }

  override def decode(value : String) : Any = throw new UnsupportedOperationException("this method should not have been called")

  override def encode(value: Any): String = {
    value match {
      case t: Timestamp => formatter.print(new DateTime(t))
      case t: Date => formatter.print(new DateTime(t))
      case t: Calendar => formatter.print(new DateTime(t))
      case t: LocalDateTime => formatter.print(t)
      case t: ReadableDateTime => formatter.print(t)
      case _ => throw new DateEncoderNotAvailableException(value)
    }
  }

  override def supportsStringDecoding : Boolean = false

}
