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

package io.github.mavenrain.async.db.mysql

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import io.github.mavenrain.async.db._
import io.github.mavenrain.async.db.exceptions._
import io.github.mavenrain.async.db.mysql.codec.{MySQLConnectionHandler, MySQLHandlerDelegate}
import io.github.mavenrain.async.db.mysql.exceptions.MySQLException
import io.github.mavenrain.async.db.mysql.message.client._
import io.github.mavenrain.async.db.mysql.message.server._
import io.github.mavenrain.async.db.mysql.util.CharsetMapper
import io.github.mavenrain.async.db.pool.TimeoutScheduler
import io.github.mavenrain.async.db.util.ChannelFutureTransformer.toFuture
import io.github.mavenrain.async.db.util._
import io.netty.channel.{ChannelHandlerContext, EventLoopGroup}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object MySQLConnection {
  final val Counter = new AtomicLong()
  final val MicrosecondsVersion = Version(5,6,0)
  final val log = Log.get[MySQLConnection]
}

class MySQLConnection(
  configuration: Configuration,
  charsetMapper: CharsetMapper = CharsetMapper.Instance,
  group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
  implicit val executionContext : ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
) extends MySQLHandlerDelegate
  with Connection
  with TimeoutScheduler
{

  import MySQLConnection.log

  // validate that this charset is supported
  charsetMapper.toInt(configuration.charset)

  private final val connectionCount = MySQLConnection.Counter.incrementAndGet()
  private final val connectionId = s"[mysql-connection-$connectionCount]"

  private final val connectionHandler = new MySQLConnectionHandler(
    configuration,
    charsetMapper,
    this,
    group,
    executionContext,
    connectionId)

  private final val connectionPromise = Promise[Connection]()
  private final val disconnectionPromise = Promise[Connection]()

  private val queryPromiseReference = new AtomicReference[Option[Promise[QueryResult]]](None)
  private var connected = false
  private var _lastException : Throwable = null
  private var serverVersion : Version = null

  def version = serverVersion
  def lastException : Throwable = _lastException
  def count : Long = connectionCount

  override def eventLoopGroup : EventLoopGroup = group

  def connect: Future[Connection] = {
    connectionHandler.connect.failed.foreach(
      connectionPromise.tryFailure(_)
    )

    connectionPromise.future
  }

  def close: Future[Connection] = {
    if ( isConnected ) {
      if (!disconnectionPromise.isCompleted) {
        val exception = new DatabaseException("Connection is being closed")
        exception.fillInStackTrace()
        failQueryPromise(exception)
        connectionHandler.clearQueryState
        connectionHandler.write(QuitMessage.Instance).onComplete {
          case Success(channelFuture) => {
            connectionHandler.disconnect.onComplete {
              case Success(closeFuture) => disconnectionPromise.trySuccess(this)
              case Failure(e) => disconnectionPromise.tryFailure(e)
            }
          }
          case Failure(exception) => disconnectionPromise.tryFailure(exception)
        }
      }
    }

    disconnectionPromise.future
  }

  override def connected(ctx: ChannelHandlerContext): Unit = {
    log.debug("Connected to {}", ctx.channel.remoteAddress)
    connected = true
  }

  override def exceptionCaught(throwable: Throwable): Unit = {
    log.error("Transport failure ", throwable)
    setException(throwable)
  }

  override def onError(message: ErrorMessage): Unit = {
    log.error("Received an error message -> {}", message)
    val exception = new MySQLException(message)
    exception.fillInStackTrace()
    setException(exception)
  }

  private def setException( t : Throwable ): Unit = {
    _lastException = t
    connectionPromise.tryFailure(t)
    failQueryPromise(t)
  }

  override def onOk(message: OkMessage): Unit = {
    if ( !connectionPromise.isCompleted ) {
      log.debug("Connected to database")
      connectionPromise.success(this)
    } else {
      if (isQuerying) {
        succeedQueryPromise(
          new MySQLQueryResult(
            message.affectedRows,
            message.message,
            message.lastInsertId,
            message.statusFlags,
            message.warnings
          )
        )
      } else {
        log.warn("Received OK when not querying or connecting, not sure what this is")
      }
    }
  }

  def onEOF(message: EOFMessage): Unit = {
    if (isQuerying) {
      succeedQueryPromise(
        new MySQLQueryResult(
          0,
          null,
          -1,
          message.flags,
          message.warningCount
        )
      )
    }
  }

  override def onHandshake(message: HandshakeMessage): Unit = {
    serverVersion = Version(message.serverVersion)

    connectionHandler.write(new HandshakeResponseMessage(
      configuration.username,
      configuration.charset,
      message.seed,
      message.authenticationMethod,
      database = configuration.database,
      password = configuration.password
    ))
  }

  override def switchAuthentication( message : AuthenticationSwitchRequest ) =
    connectionHandler.write(new AuthenticationSwitchResponse( configuration.password, message ))

  def sendQuery(query: String): Future[QueryResult] = {
    validateIsReadyForQuery()
    val promise = Promise[QueryResult]()
    setQueryPromise(promise)
    connectionHandler.write(new QueryMessage(query))
    addTimeout(promise, configuration.queryTimeout)
    promise.future
  }

  private def failQueryPromise(t: Throwable) =
    clearQueryPromise.foreach {
      _.tryFailure(t)
    }

  private def succeedQueryPromise(queryResult: QueryResult) =
    clearQueryPromise.foreach {
      _.success(queryResult)
    }

  def isQuerying: Boolean = queryPromise.isDefined

  def onResultSet(resultSet: ResultSet, message: EOFMessage): Unit =
    if (isQuerying) {
      succeedQueryPromise(
        new MySQLQueryResult(
          resultSet.size,
          null,
          -1,
          message.flags,
          message.warningCount,
          Some(resultSet)
        )
      )
    }

  def disconnect: Future[Connection] = close
  override def onTimeout = disconnect

  def isConnected: Boolean = connectionHandler.isConnected

  def sendPreparedStatement(query: String, values: Seq[Any]): Future[QueryResult] = {
    validateIsReadyForQuery()
    val totalParameters = query.count( _ == '?')
    if ( values.length != totalParameters ) {
      throw new InsufficientParametersException(totalParameters, values)
    }
    val promise = Promise[QueryResult]()
    setQueryPromise(promise)
    connectionHandler.sendPreparedStatement(query, values)
    addTimeout(promise,configuration.queryTimeout)
    promise.future
  }


  override def toString: String =
    "%s(%s,%d)".format(getClass.getName, connectionId, connectionCount)

  private def validateIsReadyForQuery() =
    if ( isQuerying ) throw new ConnectionStillRunningQueryException(connectionCount, false)


  private def queryPromise: Option[Promise[QueryResult]] = queryPromiseReference.get()

  private def setQueryPromise(promise: Promise[QueryResult]): Unit =
    if (!queryPromiseReference.compareAndSet(None, Some(promise)))
      throw new ConnectionStillRunningQueryException(connectionCount, true)

  private def clearQueryPromise : Option[Promise[QueryResult]] =
    queryPromiseReference.getAndSet(None)

}
