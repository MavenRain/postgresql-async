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

package io.github.mavenrain.async.db.postgresql.codec

import io.github.mavenrain.async.db.Configuration
import io.github.mavenrain.async.db.SSLConfiguration.Mode
import io.github.mavenrain.async.db.column.{ColumnDecoderRegistry, ColumnEncoderRegistry}
import io.github.mavenrain.async.db.postgresql.exceptions._
import io.github.mavenrain.async.db.postgresql.messages.backend._
import io.github.mavenrain.async.db.postgresql.messages.frontend._
import io.github.mavenrain.async.db.util.ChannelFutureTransformer.toFuture
import io.github.mavenrain.async.db.util._
import java.net.InetSocketAddress
import scala.annotation.switch
import scala.concurrent._
import io.netty.channel._
import io.netty.bootstrap.Bootstrap
import io.netty.channel
import scala.util.Failure
import io.github.mavenrain.async.db.postgresql.messages.backend.DataRowMessage
import io.github.mavenrain.async.db.postgresql.messages.backend.CommandCompleteMessage
import io.github.mavenrain.async.db.postgresql.messages.backend.ProcessData
import scala.util.Success
import io.github.mavenrain.async.db.postgresql.messages.backend.RowDescriptionMessage
import io.github.mavenrain.async.db.postgresql.messages.backend.ParameterStatusMessage
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import io.netty.handler.ssl.{SslContextBuilder, SslHandler}
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.util.concurrent.FutureListener
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.io.FileInputStream

object PostgreSQLConnectionHandler {
  final val log = Log.get[PostgreSQLConnectionHandler]
}

class PostgreSQLConnectionHandler
(
  configuration: Configuration,
  encoderRegistry: ColumnEncoderRegistry,
  decoderRegistry: ColumnDecoderRegistry,
  connectionDelegate : PostgreSQLConnectionDelegate,
  group : EventLoopGroup,
  executionContext : ExecutionContext
  )
  extends SimpleChannelInboundHandler[Object]
{

  import PostgreSQLConnectionHandler.log

  private val properties = List(
    "user" -> configuration.username,
    "database" -> configuration.database,
    "client_encoding" -> configuration.charset.name(),
    "DateStyle" -> "ISO",
    "extra_float_digits" -> "2")

  private implicit final val _executionContext = executionContext
  private final val bootstrap = new Bootstrap()
  private final val connectionFuture = Promise[PostgreSQLConnectionHandler]()
  private final val disconnectionPromise = Promise[PostgreSQLConnectionHandler]()
  protected var processData : ProcessData = null

  private var currentContext : ChannelHandlerContext = null

  def connect: Future[PostgreSQLConnectionHandler] = {
    bootstrap.group(group)
    bootstrap.channel(classOf[NioSocketChannel])
    bootstrap.handler(new ChannelInitializer[channel.Channel]() {

      override def initChannel(ch: channel.Channel): Unit = {
        ch.pipeline.addLast(
          new MessageDecoder(configuration.ssl.mode != Mode.Disable, configuration.charset, configuration.maximumMessageSize),
          new MessageEncoder(configuration.charset, encoderRegistry),
          PostgreSQLConnectionHandler.this)
      }

    })

    bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    bootstrap.option(ChannelOption.ALLOCATOR, configuration.allocator)

    bootstrap.connect(new InetSocketAddress(configuration.host, configuration.port)).failed.foreach(
      connectionFuture.tryFailure(_)
    )

    connectionFuture.future
  }

  def disconnect: Future[PostgreSQLConnectionHandler] = {

    if ( isConnected ) {
      currentContext.channel.writeAndFlush(CloseMessage).onComplete {
        case Success(writeFuture) => writeFuture.channel.close().onComplete {
          case Success(closeFuture) => disconnectionPromise.trySuccess(this)
          case Failure(e) => disconnectionPromise.tryFailure(e)
        }
        case Failure(e) => disconnectionPromise.tryFailure(e)
      }
    }

    disconnectionPromise.future
  }

  def isConnected: Boolean = {
    if (currentContext != null) {
      currentContext.channel.isActive
    } else {
      false
    }
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    if (configuration.ssl.mode == Mode.Disable)
      ctx.writeAndFlush(new StartupMessage(properties))
    else
      ctx.writeAndFlush(SSLRequestMessage)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Object): Unit = {

    msg match {

      case SSLResponseMessage(supported) =>
        if (supported) {
          val ctxBuilder = SslContextBuilder.forClient()
          if (configuration.ssl.mode >= Mode.VerifyCA) {
            configuration.ssl.rootCert.fold {
              val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
              val ks = KeyStore.getInstance(KeyStore.getDefaultType())
              val cacerts = new FileInputStream(System.getProperty("java.home") + "/lib/security/cacerts")
              try {
                ks.load(cacerts, "changeit".toCharArray)
              } finally {
                cacerts.close()
              }
              tmf.init(ks)
              ctxBuilder.trustManager(tmf)
            } { path =>
              ctxBuilder.trustManager(path)
            }
          } else {
            ctxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
          }
          val sslContext = ctxBuilder.build()
          val sslEngine = sslContext.newEngine(ctx.alloc(), configuration.host, configuration.port)
          if (configuration.ssl.mode >= Mode.VerifyFull) {
            val sslParams = sslEngine.getSSLParameters()
            sslParams.setEndpointIdentificationAlgorithm("HTTPS")
            sslEngine.setSSLParameters(sslParams)
          }
          val handler = new SslHandler(sslEngine)
          ctx.pipeline().addFirst(handler)
          handler.handshakeFuture.addListener(new FutureListener[channel.Channel]() {
            def operationComplete(future: io.netty.util.concurrent.Future[channel.Channel]) =
              if (future.isSuccess())
                ctx.writeAndFlush(new StartupMessage(properties))
              else
                connectionDelegate.onError(future.cause())
          })
        } else if (configuration.ssl.mode < Mode.Require) {
          ctx.writeAndFlush(new StartupMessage(properties))
        } else {
          connectionDelegate.onError(new IllegalArgumentException("SSL is not supported on server"))
        }

      case m: ServerMessage => {

        (m.kind : @switch) match {
          case ServerMessage.BackendKeyData => {
            processData = m.asInstanceOf[ProcessData]
          }
          case ServerMessage.BindComplete => {
          }
          case ServerMessage.Authentication => {
            log.debug("Authentication response received {}", m)
            connectionDelegate.onAuthenticationResponse(m.asInstanceOf[AuthenticationMessage])
          }
          case ServerMessage.CommandComplete => {
            connectionDelegate.onCommandComplete(m.asInstanceOf[CommandCompleteMessage])
          }
          case ServerMessage.CloseComplete => {
          }
          case ServerMessage.DataRow => {
            connectionDelegate.onDataRow(m.asInstanceOf[DataRowMessage])
          }
          case ServerMessage.Error => {
            connectionDelegate.onError(m.asInstanceOf[ErrorMessage])
          }
          case ServerMessage.EmptyQueryString => {
            val exception = new QueryMustNotBeNullOrEmptyException(null)
            exception.fillInStackTrace()
            connectionDelegate.onError(exception)
          }
          case ServerMessage.NoData => {
          }
          case ServerMessage.Notice => {
            log.info("Received notice {}", m)
          }
          case ServerMessage.NotificationResponse => {
            connectionDelegate.onNotificationResponse(m.asInstanceOf[NotificationResponse])
          }
          case ServerMessage.ParameterStatus => {
            connectionDelegate.onParameterStatus(m.asInstanceOf[ParameterStatusMessage])
          }
          case ServerMessage.ParseComplete => {
          }
          case ServerMessage.ReadyForQuery => {
            connectionDelegate.onReadyForQuery()
          }
          case ServerMessage.RowDescription => {
            connectionDelegate.onRowDescription(m.asInstanceOf[RowDescriptionMessage])
          }
          case _ => {
            val exception = new IllegalStateException("Handler not implemented for message %s".format(m.kind))
            exception.fillInStackTrace()
            connectionDelegate.onError(exception)
          }
        }

      }
      case _ => {
        log.error("Unknown message type - {}", msg)
        val exception = new IllegalArgumentException("Unknown message type - %s".format(msg))
        exception.fillInStackTrace()
        connectionDelegate.onError(exception)
      }

    }

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) =
    // unwrap CodecException if needed
    cause match {
      case t: CodecException => connectionDelegate.onError(t.getCause)
      case _ =>  connectionDelegate.onError(cause)
    }

  override def channelInactive(ctx: ChannelHandlerContext): Unit =
    log.info("Connection disconnected - {}", ctx.channel.remoteAddress)

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    currentContext = ctx
  }

  def write( message : ClientMessage ) =
    currentContext.writeAndFlush(message).failed.foreach(
      connectionDelegate.onError(_)
    )

}
