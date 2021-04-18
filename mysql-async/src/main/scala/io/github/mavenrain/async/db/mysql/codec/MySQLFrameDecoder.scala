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

import io.github.mavenrain.async.db.exceptions._
import io.github.mavenrain.async.db.mysql.decoder._
import io.github.mavenrain.async.db.mysql.message.server._
import io.github.mavenrain.async.db.util.ByteBufferUtils.read3BytesInt
import io.github.mavenrain.async.db.util.ChannelWrapper.bufferToWrapper
import io.github.mavenrain.async.db.util.{BufferDumper, Log}
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger


class MySQLFrameDecoder(charset: Charset, connectionId: String) extends ByteToMessageDecoder {

  private final val log = Log.getByName(s"[frame-decoder]${connectionId}")
  private final val messagesCount = new AtomicInteger()
  private final val handshakeDecoder = new HandshakeV10Decoder(charset)
  private final val errorDecoder = new ErrorDecoder(charset)
  private final val okDecoder = new OkDecoder(charset)
  private final val columnDecoder = new ColumnDefinitionDecoder(charset, new DecoderRegistry(charset))
  private final val rowDecoder = new ResultSetRowDecoder(charset)
  private final val preparedStatementPrepareDecoder = new PreparedStatementPrepareResponseDecoder()
  private final val authenticationSwitchDecoder = new AuthenticationSwitchRequestDecoder(charset)

  private[codec] var processingColumns = false
  private[codec] var processingParams = false
  private[codec] var isInQuery = false
  private[codec] var isPreparedStatementPrepare = false
  private[codec] var isPreparedStatementExecute = false
  private[codec] var isPreparedStatementExecuteRows = false
  private[codec] var hasDoneHandshake = false

  private[codec] var totalParams = 0L
  private[codec] var processedParams = 0L
  private[codec] var totalColumns = 0L
  private[codec] var processedColumns = 0L

  private var hasReadColumnsCount = false

  def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: java.util.List[Object]): Unit = {
    if (buffer.readableBytes() > 4) {

      buffer.markReaderIndex()

      val size = read3BytesInt(buffer)

      val sequence = buffer.readUnsignedByte() // we have to read this

      if (buffer.readableBytes() >= size) {

        messagesCount.incrementAndGet()

        val messageType = buffer.getByte(buffer.readerIndex())

        if (size < 0) {
          throw new NegativeMessageSizeException(messageType, size)
        }

        val slice = buffer.readSlice(size)

        if (log.isTraceEnabled) {
          log.trace(s"Reading message type $messageType - " +
            s"(count=$messagesCount,hasDoneHandshake=$hasDoneHandshake,size=$size,isInQuery=$isInQuery,processingColumns=$processingColumns,processingParams=$processingParams,processedColumns=$processedColumns,processedParams=$processedParams)" +
            s"\n${BufferDumper.dumpAsHex(slice)}}")
        }

        slice.readByte()

        if (hasDoneHandshake) {
          handleCommonFlow(messageType, slice, out)
        } else {
          val decoder = messageType match {
            case ServerMessage.Error => {
              clear
              errorDecoder
            }
            case _ => handshakeDecoder
          }
          doDecoding(decoder, slice, out)
        }
      } else {
        buffer.resetReaderIndex()
      }

    }
  }

  private def handleCommonFlow(messageType: Byte, slice: ByteBuf, out: java.util.List[Object]) = {
    val decoder = messageType match {
      case ServerMessage.Error => {
        clear
        errorDecoder
      }
      case ServerMessage.EOF => {

        if (processingParams && totalParams > 0) {
          processingParams = false
          if (totalColumns == 0) {
            ParamAndColumnProcessingFinishedDecoder
          } else {
            ParamProcessingFinishedDecoder
          }
        } else {
          if (processingColumns) {
            processingColumns = false
            ColumnProcessingFinishedDecoder
          } else {
            clear
            EOFMessageDecoder
          }
        }

      }
      case ServerMessage.Ok => {
        if (isPreparedStatementPrepare) {
          preparedStatementPrepareDecoder
        } else {
          if (isPreparedStatementExecuteRows) {
            null
          } else {
            clear
            okDecoder
          }
        }
      }
      case _ => {

        if (isInQuery) {
          null
        } else {
          throw new ParserNotAvailableException(messageType)
        }

      }
    }

    doDecoding(decoder, slice, out)
  }

  private def doDecoding(decoder: MessageDecoder, slice: ByteBuf, out: java.util.List[Object]) = {
    if (decoder == null) {
      slice.readerIndex(slice.readerIndex() - 1)
      val result = decodeQueryResult(slice)

      if (slice.readableBytes() != 0) {
        throw new BufferNotFullyConsumedException(slice)
      }
      if (result != null) {
        out.add(result)
      }
    } else {
      val result = decoder.decode(slice)

      result match {
        case m: PreparedStatementPrepareResponse => {
          hasReadColumnsCount = true
          totalColumns = m.columnsCount
          totalParams = m.paramsCount
        }
        case m: ParamAndColumnProcessingFinishedMessage => {
          clear
        }
        case m: ColumnProcessingFinishedMessage if isPreparedStatementPrepare => {
          clear
        }
        case m: ColumnProcessingFinishedMessage if isPreparedStatementExecute => {
          isPreparedStatementExecuteRows = true
        }
        case _ =>
      }

      if (slice.readableBytes() != 0) {
        throw new BufferNotFullyConsumedException(slice)
      }

      if (result != null) {
        result match {
          case m: PreparedStatementPrepareResponse => {
            out.add(result)
            if (m.columnsCount == 0 && m.paramsCount == 0) {
              clear
              out.add(new ParamAndColumnProcessingFinishedMessage(new EOFMessage(0, 0)))
            }
          }
          case _ => out.add(result)
        }
      }
    }
  }

  private def decodeQueryResult(slice: ByteBuf): AnyRef = {
    if (!hasReadColumnsCount) {
      hasReadColumnsCount = true
      totalColumns = slice.readBinaryLength
      return null
    }

    if (processingParams && totalParams != processedParams) {
      processedParams += 1
      return columnDecoder.decode(slice)
    }


    if (totalColumns == processedColumns) {
      if (isPreparedStatementExecute) {
        val row = slice.readBytes(slice.readableBytes())
        row.readByte() // reads initial 00 at message
        new BinaryRowMessage(row)
      } else {
        rowDecoder.decode(slice)
      }
    } else {
      processedColumns += 1
      columnDecoder.decode(slice)
    }

  }

  def preparedStatementPrepareStarted() = {
    queryProcessStarted()
    hasReadColumnsCount = true
    processingParams = true
    processingColumns = true
    isPreparedStatementPrepare = true
  }

  def preparedStatementExecuteStarted(columnsCount: Int, paramsCount: Int) = {
    queryProcessStarted()
    hasReadColumnsCount = false
    totalColumns = columnsCount
    totalParams = paramsCount
    isPreparedStatementExecute = true
    processingParams = false
  }

  def queryProcessStarted(): Unit = {
    isInQuery = true
    processingColumns = true
    hasReadColumnsCount = false
  }

  private def clear = {
    isPreparedStatementPrepare = false
    isPreparedStatementExecute = false
    isPreparedStatementExecuteRows = false
    isInQuery = false
    processingColumns = false
    processingParams = false
    totalColumns = 0
    processedColumns = 0
    totalParams = 0
    processedParams = 0
    hasReadColumnsCount = false
  }

}
