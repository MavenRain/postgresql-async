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

import io.github.mavenrain.async.db.exceptions.UnknownLengthException
import io.netty.buffer.ByteBuf
import java.nio.charset.Charset
import scala.language.implicitConversions
import scala.util.chaining.scalaUtilChainingOps

object ChannelWrapper {
  implicit def bufferToWrapper( buffer : ByteBuf ) = new ChannelWrapper(buffer)

  final val MySQL_NULL = 0xfb
  final val log = Log.get[ChannelWrapper]

}

class ChannelWrapper( val buffer : ByteBuf ) extends AnyVal {

  import ChannelWrapper._

  def readFixedString( length : Int, charset : Charset ) : String =
    new String( new Array[Byte](length).tap(buffer.readBytes(_)), charset )

  def readCString( charset : Charset ) = ByteBufferUtils.readCString(buffer, charset)

  def readUntilEOF( charset: Charset ) = ByteBufferUtils.readUntilEOF(buffer, charset)

  def readLengthEncodedString( charset : Charset ) : String =
    readFixedString(readBinaryLength.pipe(_.asInstanceOf[Int]), charset)

  def readBinaryLength : Long = {
    val firstByte = buffer.readUnsignedByte()

    if ( firstByte <= 250 ) {
      firstByte
    } else {
      firstByte match {
        case MySQL_NULL => -1
        case 252 => buffer.readUnsignedShort()
        case 253 => readLongInt
        case 254 => buffer.readLong()
        case _ => throw new UnknownLengthException(firstByte)
      }
    }

  }

  def readLongInt : Int = {
    val first = buffer.readByte()
    val second = buffer.readByte()
    val third = buffer.readByte()

    ( first & 0xff ) | (( second & 0xff ) << 8) | ((third & 0xff) << 16)
  }

  def writeLength( length : Long ): Unit =
    if (length < 251) {
      buffer.writeByte( length.asInstanceOf[Byte])
    } else if (length < 65536L) {
      buffer.writeByte(252)
      buffer.writeShort(length.asInstanceOf[Int])
    } else if (length < 16777216L) {
      buffer.writeByte(253)
      writeLongInt(length.asInstanceOf[Int])
    } else {
      buffer.writeByte(254)
      buffer.writeLong(length)
    }

  def writeLongInt(i : Int): Unit =
    buffer
      .tap(_.writeByte( i & 0xff ))
      .tap(_.writeByte( i >>> 8 ))
      .tap(_.writeByte( i >>> 16 ))

  def writeLenghtEncodedString( value : String, charset : Charset ): Unit =
    value
      .getBytes(charset)
      .pipe(bytes =>
        writeLength(bytes.length).tap(_ => buffer.writeBytes(bytes))
      )

  def writePacketLength( sequence : Int = 0 ) =
    ByteBufferUtils.writePacketLength(buffer, sequence )

  def mysqlReadInt() : Int = {
    val first = buffer.readByte()
    val last = buffer.readByte()

    (first & 0xff) | ((last & 0xff) << 8)
  }


}
