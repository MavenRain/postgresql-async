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

import io.github.mavenrain.async.db.Configuration
import io.github.mavenrain.async.db.exceptions.DatabaseException
import io.github.mavenrain.async.db.general.MutableResultSet
import io.github.mavenrain.async.db.mysql.binary.BinaryRowDecoder
import io.github.mavenrain.async.db.mysql.message.client._
import io.github.mavenrain.async.db.mysql.message.server._
import io.github.mavenrain.async.db.mysql.util.CharsetMapper
import io.github.mavenrain.async.db.util.ChannelFutureTransformer.toFuture
import io.github.mavenrain.async.db.util._
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}
import io.netty.channel._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import scala.annotation.switch
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent._
import scala.concurrent.duration.Duration

class MySQLConnectionHandler(
                              configuration: Configuration,
                              charsetMapper: CharsetMapper,
                              handlerDelegate: MySQLHandlerDelegate,
                              group : EventLoopGroup,
                              executionContext : ExecutionContext,
                              connectionId : String
                              )
  extends SimpleChannelInboundHandler[Object] {

  private implicit val internalPool = executionContext
  private final val log = Log.getByName(s"[connection-handler]${connectionId}")
  private final val bootstrap = new Bootstrap().group(group)
  private final val connectionPromise = Promise[MySQLConnectionHandler]()
  private final val decoder = new MySQLFrameDecoder(configuration.charset, connectionId)
  private final val encoder = new MySQLOneToOneEncoder(configuration.charset, charsetMapper)
  private final val sendLongDataEncoder = new SendLongDataEncoder()
  private final val currentParameters = new ArrayBuffer[ColumnDefinitionMessage]()
  private final val currentColumns = new ArrayBuffer[ColumnDefinitionMessage]()
  private final val parsedStatements = new HashMap[String,PreparedStatementHolder]()
  private final val binaryRowDecoder = new BinaryRowDecoder()

  private var currentPreparedStatementHolder : PreparedStatementHolder = null
  private var currentPreparedStatement : PreparedStatement = null
  private var currentQuery : MutableResultSet[ColumnDefinitionMessage] = null
  private var currentContext: ChannelHandlerContext = null

  def connect: Future[MySQLConnectionHandler] = {
    bootstrap.channel(classOf[NioSocketChannel])
    bootstrap.handler(new ChannelInitializer[io.netty.channel.Channel]() {

      override def initChannel(channel: io.netty.channel.Channel): Unit = {
        channel.pipeline.addLast(
          decoder,
          encoder,
          sendLongDataEncoder,
          MySQLConnectionHandler.this)
      }

    })

    bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    bootstrap.option[ByteBufAllocator](ChannelOption.ALLOCATOR, LittleEndianByteBufAllocator.INSTANCE)

    bootstrap.connect(new InetSocketAddress(configuration.host, configuration.port)).failed.foreach {
      connectionPromise.tryFailure(_)
    }

    connectionPromise.future
  }

  override def channelRead0(ctx: ChannelHandlerContext, message: Object) =
    message match {
      case m: ServerMessage =>
        (m.kind: @switch) match {
          case ServerMessage.ServerProtocolVersion =>
            handlerDelegate.onHandshake(m.asInstanceOf[HandshakeMessage])
          case ServerMessage.Ok => {
            clearQueryState
            handlerDelegate.onOk(m.asInstanceOf[OkMessage])
          }
          case ServerMessage.Error => {
            clearQueryState
            handlerDelegate.onError(m.asInstanceOf[ErrorMessage])
          }
          case ServerMessage.EOF =>
            handleEOF(m)
          case ServerMessage.ColumnDefinition => {
            val message = m.asInstanceOf[ColumnDefinitionMessage]

            if ( currentPreparedStatementHolder != null && currentPreparedStatementHolder.needsAny ) {
              currentPreparedStatementHolder.add(message)
            }

            currentColumns += message
          }
          case ServerMessage.ColumnDefinitionFinished =>
            onColumnDefinitionFinished()
          case ServerMessage.PreparedStatementPrepareResponse =>
            onPreparedStatementPrepareResponse(m.asInstanceOf[PreparedStatementPrepareResponse])
          case ServerMessage.Row => {
            val message = m.asInstanceOf[ResultSetRowMessage]
            val items = new Array[Any](message.size)

            var x = 0
            while ( x < message.size ) {
              items(x) = if ( message(x) == null ) {
                null
              } else {
                val columnDescription = currentQuery.columnTypes(x)
                columnDescription.textDecoder.decode(columnDescription, message(x), configuration.charset)
              }
              x += 1
            }

            currentQuery.addRow(items)
          }
          case ServerMessage.BinaryRow => {
            val message = m.asInstanceOf[BinaryRowMessage]
            currentQuery.addRow(binaryRowDecoder.decode(message.buffer, currentColumns.toSeq ))
          }
          case ServerMessage.ParamProcessingFinished => ()
          case ServerMessage.ParamAndColumnProcessingFinished =>
            onColumnDefinitionFinished()
        }
    }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log.debug("Channel became active")
    handlerDelegate.connected(ctx)
  }


  override def channelInactive(ctx: ChannelHandlerContext) =
    log.debug("Channel became inactive")

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) =
    // unwrap CodecException if needed
    cause match {
      case t: CodecException => handleException(t.getCause)
      case _ =>  handleException(cause)
    }

  private def handleException(cause: Throwable) = {
    if (!connectionPromise.isCompleted) {
      connectionPromise.failure(cause)
    }
    handlerDelegate.exceptionCaught(cause)
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    currentContext = ctx
  }

  def write( message : QueryMessage ) : ChannelFuture = {
    decoder.queryProcessStarted()
    writeAndHandleError(message)
  }

  def sendPreparedStatement( query: String, values: Seq[Any] ): Future[ChannelFuture] = {
    val preparedStatement = new PreparedStatement(query, values)
    currentColumns.clear()
    currentParameters.clear()
    currentPreparedStatement = preparedStatement
    parsedStatements.get(preparedStatement.statement) match {
      case Some( item ) =>
        executePreparedStatement(item.statementId, item.columns.size, preparedStatement.values, item.parameters.toSeq)
      case None => {
        decoder.preparedStatementPrepareStarted()
        writeAndHandleError( new PreparedStatementPrepareMessage(preparedStatement.statement) )
      }
    }
  }

  def write( message : HandshakeResponseMessage ) : ChannelFuture = {
    decoder.hasDoneHandshake = true
    writeAndHandleError(message)
  }

  def write( message : AuthenticationSwitchResponse ) : ChannelFuture = writeAndHandleError(message)

  def write( message : QuitMessage ) : ChannelFuture =
    writeAndHandleError(message)

  def disconnect: ChannelFuture = currentContext.close()

  def clearQueryState: Unit = {
    currentColumns.clear()
    currentParameters.clear()
    currentQuery = null
  }

  def isConnected : Boolean =
    if ( currentContext != null && currentContext.channel() != null )
      currentContext.channel.isActive
    else
      false

  private def executePreparedStatement( statementId : Array[Byte], columnsCount : Int, values : Seq[Any], parameters : Seq[ColumnDefinitionMessage] ): Future[ChannelFuture] = {
    decoder.preparedStatementExecuteStarted(columnsCount, parameters.size)
    currentColumns.clear()
    currentParameters.clear()

    val (nonLongIndicesOpt, longValuesOpt) = values.zipWithIndex.map {
      case (Some(value), index) if isLong(value) => (None, Some(index, value))
      case (value, index) if isLong(value) => (None, Some(index, value))
      case (_, index) => (Some(index), None)
    }.unzip
    val nonLongIndices: Seq[Int] = nonLongIndicesOpt.flatten
    val longValues: Seq[(Int, Any)] = longValuesOpt.flatten

    if (longValues.nonEmpty) {
      val (firstIndex, firstValue) = longValues.head
      var channelFuture: Future[ChannelFuture] = sendLongParameter(statementId, firstIndex, firstValue)
      longValues.tail foreach { case (index, value) =>
        channelFuture = channelFuture.flatMap { _ =>
          sendLongParameter(statementId, index, value)
        }
      }
      channelFuture flatMap { _ =>
        writeAndHandleError(new PreparedStatementExecuteMessage(statementId, values, nonLongIndices.toSet, parameters))
      }
    } else {
      writeAndHandleError(new PreparedStatementExecuteMessage(statementId, values, nonLongIndices.toSet, parameters))
    }
  }

  private def isLong(value: Any): Boolean = {
    value match {
      case v : Array[Byte] => v.length > SendLongDataEncoder.LONG_THRESHOLD
      case v : ByteBuffer => v.remaining() > SendLongDataEncoder.LONG_THRESHOLD
      case v : ByteBuf => v.readableBytes() > SendLongDataEncoder.LONG_THRESHOLD

      case _ => false
    }
  }

  private def sendLongParameter(statementId: Array[Byte], index: Int, longValue: Any): Future[ChannelFuture] = {
    longValue match {
      case v : Array[Byte] =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v : ByteBuffer =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v : ByteBuf =>
        sendBuffer(v, statementId, index)
    }
  }

  private def sendBuffer(buffer: ByteBuf, statementId: Array[Byte], paramId: Int): ChannelFuture = {
    writeAndHandleError(new SendLongDataMessage(statementId, buffer, paramId))
  }

  private def onPreparedStatementPrepareResponse( message : PreparedStatementPrepareResponse ) = {
    currentPreparedStatementHolder = new PreparedStatementHolder( currentPreparedStatement.statement, message)
  }

  def onColumnDefinitionFinished(): Unit = {

    val columns =
      if ( currentPreparedStatementHolder != null )
        currentPreparedStatementHolder.columns
      else
        currentColumns

    currentQuery = new MutableResultSet[ColumnDefinitionMessage](columns.toIndexedSeq)

    if ( currentPreparedStatementHolder != null ) {
      parsedStatements.put( currentPreparedStatementHolder.statement, currentPreparedStatementHolder )
      executePreparedStatement(
        currentPreparedStatementHolder.statementId,
        currentPreparedStatementHolder.columns.size,
        currentPreparedStatement.values,
        currentPreparedStatementHolder.parameters.toSeq
      )
      currentPreparedStatementHolder = null
      currentPreparedStatement = null
    }
  }

  private def writeAndHandleError( message : Any ) : ChannelFuture = 
    if ( currentContext.channel().isActive ) {
      val res = currentContext.writeAndFlush(message)

      res.failed.foreach(handleException(_))

      res
    } else {
      val error = new DatabaseException("This channel is not active and can't take messages")
      handleException(error)
      currentContext.channel().newFailedFuture(error)
    }

  private def handleEOF( m : ServerMessage ) =
    m match {
      case eof : EOFMessage => {
        val resultSet = currentQuery
        clearQueryState

        if ( resultSet != null )
          handlerDelegate.onResultSet( resultSet, eof )
       else
          handlerDelegate.onEOF(eof)
      }
      case authenticationSwitch : AuthenticationSwitchRequest =>
        handlerDelegate.switchAuthentication(authenticationSwitch)
    }

  def schedule(block: => Unit, duration: Duration): Unit = {
    currentContext.channel().eventLoop().schedule(new Runnable {
      override def run(): Unit = block
    }, duration.toMillis, TimeUnit.MILLISECONDS)
  }

}
