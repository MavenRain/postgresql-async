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

package io.github.mavenrain.async.db.util

import io.netty.buffer.{Unpooled, ByteBuf}
import java.nio.charset.Charset
import scala.util.chaining.scalaUtilChainingOps

object ByteBufferUtils {

  def writeLength(buffer: ByteBuf) =
    (buffer.writerIndex() - 1)
      .pipe(length =>
        length
          .tap(_ => buffer.markWriterIndex())
          .tap(_ => buffer.writerIndex(1))
          .pipe(buffer.writeInt(_))
          .tap(_ => buffer.resetWriterIndex())
      )

  def writeCString(content: String, b: ByteBuf, charset: Charset): Unit =
    b
      .tap(_.writeBytes(content.getBytes(charset)))
      .tap(_.writeByte(0))

  def writeSizedString( content : String, b : ByteBuf, charset : Charset ): Unit =
    content
      .getBytes(charset)
      .pipe(bytes =>
        b
          .tap(_.writeByte(bytes.length))
          .tap(_.writeBytes(bytes))
      )

  def readCString(b: ByteBuf, charset: Charset): String = {
    b.markReaderIndex()

    var byte: Byte = 0
    var count = 0

    do {
      byte = b.readByte()
      count += 1
    } while (byte != 0)

    b.resetReaderIndex()

    val result = b.toString(b.readerIndex(), count - 1, charset)

    b.readerIndex(b.readerIndex() + count)

    result
  }

  def readUntilEOF( b : ByteBuf, charset : Charset ) : String = {
    if ( b.readableBytes() == 0 ) {
      return ""
    }

    b.markReaderIndex()

    var byte: Byte = -1
    var count = 0
    var offset = 1

    while (byte != 0) {
      if ( b.readableBytes() > 0 ) {
        byte = b.readByte()
        count += 1
      } else {
        byte = 0
        offset = 0
      }
    }

    b.resetReaderIndex()

    val result = b.toString(b.readerIndex(), count - offset, charset)

    b.readerIndex(b.readerIndex() + count)

    result
  }

  def read3BytesInt( b : ByteBuf ) : Int =
    (b.readByte() & 0xff) | ((b.readByte() & 0xff) << 8) | ((b.readByte() & 0xff) << 16)

  def write3BytesInt( b : ByteBuf, value : Int ): Unit =
    b
      .tap(_.writeByte( value & 0xff ))
      .tap(_.writeByte( value >>> 8 ))
      .tap(_.writeByte( value >>> 16 ))

  def writePacketLength(buffer: ByteBuf, sequence : Int = 1): Unit =
    (buffer.writerIndex() - 4)
      .pipe(length =>
        buffer
          .tap(_.markWriterIndex())
          .tap(_.writerIndex(0))
          .tap(_ => write3BytesInt(buffer, length))
          .tap(_.writeByte(sequence))
          .tap(_.resetWriterIndex())
      )

  def packetBuffer( estimate : Int = 1024  ) : ByteBuf =
    mysqlBuffer(estimate).tap(_.writeInt(0))

  def mysqlBuffer( estimate : Int = 1024 ) : ByteBuf =
    Unpooled.buffer(estimate)

}
