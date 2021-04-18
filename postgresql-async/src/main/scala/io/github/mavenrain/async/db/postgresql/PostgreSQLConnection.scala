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

package io.github.mavenrain.async.db.postgresql

import io.github.mavenrain.async.db.QueryResult
import io.github.mavenrain.async.db.column.{ColumnDecoderRegistry, ColumnEncoderRegistry}
import io.github.mavenrain.async.db.exceptions.{ConnectionStillRunningQueryException, InsufficientParametersException}
import io.github.mavenrain.async.db.general.MutableResultSet
import io.github.mavenrain.async.db.pool.TimeoutScheduler
import io.github.mavenrain.async.db.postgresql.codec.{PostgreSQLConnectionDelegate, PostgreSQLConnectionHandler}
import io.github.mavenrain.async.db.postgresql.column.{PostgreSQLColumnDecoderRegistry, PostgreSQLColumnEncoderRegistry}
import io.github.mavenrain.async.db.postgresql.exceptions._
import io.github.mavenrain.async.db.util._
import io.github.mavenrain.async.db.{Configuration, Connection}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import messages.backend._
import messages.frontend._

import scala.concurrent._
import io.netty.channel.EventLoopGroup
import java.util.concurrent.CopyOnWriteArrayList

import io.github.mavenrain.async.db.postgresql.util.URLParser

object PostgreSQLConnection {
  final val Counter = new AtomicLong()
  final val ServerVersionKey = "server_version"
  final val log = Log.get[PostgreSQLConnection]
}

class PostgreSQLConnection
(
  configuration: Configuration = URLParser.DEFAULT,
  encoderRegistry: ColumnEncoderRegistry = PostgreSQLColumnEncoderRegistry.Instance,
  decoderRegistry: ColumnDecoderRegistry = PostgreSQLColumnDecoderRegistry.Instance,
  group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
  implicit val executionContext : ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
  )
  extends PostgreSQLConnectionDelegate
  with Connection
  with  TimeoutScheduler {

  import PostgreSQLConnection._

  private final val connectionHandler = new PostgreSQLConnectionHandler(
    configuration,
    encoderRegistry,
    decoderRegistry,
    this,
    group,
    executionContext
  )

  private final val currentCount = Counter.incrementAndGet()
  private final val preparedStatementsCounter = new AtomicInteger()

  private val parameterStatus = new scala.collection.mutable.HashMap[String, String]()
  private val parsedStatements = new scala.collection.mutable.HashMap[String, PreparedStatementHolder]()
  private var authenticated = false

  private val connectionFuture = Promise[Connection]()

  private var recentError = false
  private val queryPromiseReference = new AtomicReference[Option[Promise[QueryResult]]](None)
  private var currentQuery: Option[MutableResultSet[PostgreSQLColumnData]] = None
  private var currentPreparedStatement: Option[PreparedStatementHolder] = None
  private var version = Version(0,0,0)
  private var notifyListeners = new CopyOnWriteArrayList[NotificationResponse => Unit]()
  
  private var queryResult: Option[QueryResult] = None

  override def eventLoopGroup : EventLoopGroup = group
  def isReadyForQuery: Boolean = queryPromise.isEmpty

  def connect: Future[Connection] = {
    connectionHandler.connect.failed.foreach(
      connectionFuture.tryFailure(_)
    )

    connectionFuture.future
  }

  override def disconnect: Future[Connection] = connectionHandler.disconnect.map( c => this )
  override def onTimeout = disconnect

  override def isConnected: Boolean = connectionHandler.isConnected

  def parameterStatuses: scala.collection.immutable.Map[String, String] = parameterStatus.toMap

  override def sendQuery(query: String): Future[QueryResult] = {
    validateQuery(query)

    val promise = Promise[QueryResult]()
    setQueryPromise(promise)

    write(new QueryMessage(query))
    addTimeout(promise,configuration.queryTimeout)
    promise.future
  }

  override def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] = {
    validateQuery(query)

    val promise = Promise[QueryResult]()
    setQueryPromise(promise)

    val holder = parsedStatements.getOrElseUpdate(query,
      new PreparedStatementHolder( query, preparedStatementsCounter.incrementAndGet ))

    if (holder.paramsCount != values.length) {
      clearQueryPromise
      throw new InsufficientParametersException(holder.paramsCount, values)
    }

    currentPreparedStatement = Some(holder)
    currentQuery = Some(new MutableResultSet(holder.columnDatas.toIndexedSeq))
    write(
      if (holder.prepared)
        new PreparedStatementExecuteMessage(holder.statementId, holder.realQuery, values, encoderRegistry)
      else {
        holder.prepared = true
        new PreparedStatementOpeningMessage(holder.statementId, holder.realQuery, values, encoderRegistry)
      })
    addTimeout(promise,configuration.queryTimeout)
    promise.future
  }

  override def onError( exception : Throwable ) =
    setErrorOnFutures(exception)

  def hasRecentError: Boolean = recentError

  private def setErrorOnFutures(e: Throwable): Unit = {
    recentError = true

    log.error("Error on connection", e)

    if (!connectionFuture.isCompleted) {
      connectionFuture.failure(e)
      disconnect
    }

    currentPreparedStatement.map(p => parsedStatements.remove(p.query))
    currentPreparedStatement = None
    failQueryPromise(e)
  }

  override def onReadyForQuery(): Unit = {
    connectionFuture.trySuccess(this)
    
    recentError = false
    queryResult.foreach(succeedQueryPromise)
  }

  override def onError(m: ErrorMessage): Unit = {
    log.error("Error with message -> {}", m)

    val error = new GenericDatabaseException(m)
    error.fillInStackTrace()

    setErrorOnFutures(error)
  }

  override def onCommandComplete(m: CommandCompleteMessage): Unit = {
    currentPreparedStatement = None
    queryResult = Some(new QueryResult(m.rowsAffected, m.statusMessage, currentQuery))
  }

  override def onParameterStatus(m: ParameterStatusMessage): Unit = {
    parameterStatus.put(m.key, m.value)
    if ( ServerVersionKey == m.key ) {
      version = Version(m.value)
    }
  }

  override def onDataRow(m: DataRowMessage): Unit = {
    val items = new Array[Any](m.values.size)
    var x = 0

    while ( x < m.values.size ) {
      val buf = m.values(x)
      items(x) = if ( buf == null ) {
        null
      } else {
        try {
          val columnType = currentQuery.get.columnTypes(x)
          decoderRegistry.decode(columnType, buf, configuration.charset)
        } finally {
          buf.release()
        }
      }
      x += 1
    }

    currentQuery.get.addRow(items)
  }

  override def onRowDescription(m: RowDescriptionMessage): Unit = {
    currentQuery = Option(new MutableResultSet(m.columnDatas.toIndexedSeq))
    setColumnDatas(m.columnDatas)
  }

  private def setColumnDatas( columnDatas : Array[PostgreSQLColumnData] ) =
    currentPreparedStatement.foreach { holder =>
      holder.columnDatas = columnDatas
    }

  override def onAuthenticationResponse(message: AuthenticationMessage) =
    message match {
      case _: AuthenticationOkMessage => {
        log.debug("Successfully logged in to database")
        authenticated = true
      }
      case m: AuthenticationChallengeCleartextMessage => {
        write(credential(m))
      }
      case m: AuthenticationChallengeMD5 => {
        write(credential(m))
      }
    }


  override def onNotificationResponse( message : NotificationResponse ): Unit = {
    val iterator = notifyListeners.iterator()
    while ( iterator.hasNext ) {
      iterator.next().apply(message)
    }
  }

  def registerNotifyListener( listener : NotificationResponse => Unit ) =
    notifyListeners.add(listener)

  def unregisterNotifyListener( listener : NotificationResponse => Unit ) =
    notifyListeners.remove(listener)

  def clearNotifyListeners() = notifyListeners.clear()

  private def credential(authenticationMessage: AuthenticationChallengeMessage): CredentialMessage =
    if (configuration.username != null && configuration.password.isDefined) {
      new CredentialMessage(
        configuration.username,
        configuration.password.get,
        authenticationMessage.challengeType,
        authenticationMessage.salt
      )
    } else {
      throw new MissingCredentialInformationException(
        configuration.username,
        configuration.password,
        authenticationMessage.challengeType)
    }

  private[this] def notReadyForQueryError(errorMessage : String, race : Boolean) = {
    log.error(errorMessage)
    throw new ConnectionStillRunningQueryException(
      currentCount,
      race
    )
  }
  
  def validateIfItIsReadyForQuery(errorMessage: String) =
    if (queryPromise.isDefined) notReadyForQueryError(errorMessage, false)
  
  private def validateQuery(query: String): Unit = {
    validateIfItIsReadyForQuery("Can't run query because there is one query pending already")

    if (query == null || query.isEmpty) {
      throw new QueryMustNotBeNullOrEmptyException(query)
    }
  }

  private def queryPromise: Option[Promise[QueryResult]] = queryPromiseReference.get()

  private def setQueryPromise(promise: Promise[QueryResult]): Unit = {
    if (!queryPromiseReference.compareAndSet(None, Some(promise)))
      notReadyForQueryError("Can't run query due to a race with another started query", true)
  }

  private def clearQueryPromise : Option[Promise[QueryResult]] =
    queryPromiseReference.getAndSet(None)

  private def failQueryPromise(t: Throwable) =
    clearQueryPromise.foreach { promise =>
      log.error("Setting error on future {}", promise)
      promise.failure(t)
    }

  private def succeedQueryPromise(result: QueryResult): Unit = {
    queryResult = None
    currentQuery = None
    clearQueryPromise.foreach {
      _.success(result)
    }
  }

  private def write( message : ClientMessage ) =
    connectionHandler.write(message)

  override def toString: String =
    s"${getClass.getSimpleName}{counter=${currentCount}}"
}
