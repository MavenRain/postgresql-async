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

package io.github.mavenrain.async.db.mysql.pool

import io.github.mavenrain.async.db.Configuration
import io.github.mavenrain.async.db.pool.ObjectFactory
import io.github.mavenrain.async.db.mysql.MySQLConnection
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import io.github.mavenrain.async.db.util.Log
import io.github.mavenrain.async.db.exceptions.{ConnectionTimeoutedException, ConnectionStillRunningQueryException, ConnectionNotConnectedException}

object MySQLConnectionFactory {
  final val log = Log.get[MySQLConnectionFactory]
}

/**
 *
 * Connection pool factory for [[io.github.mavenrain.async.db.mysql.MySQLConnection]] objects.
 *
 * @param configuration a valid configuration to connect to a MySQL server.
 *
 */

class MySQLConnectionFactory( configuration : Configuration ) extends ObjectFactory[MySQLConnection] {

  import MySQLConnectionFactory.log

  /**
   *
   * Creates a valid object to be used in the pool. This method can block if necessary to make sure a correctly built
   * is created.
   *
   * @return
   */
  def create: MySQLConnection = {
    val connection = new MySQLConnection(configuration)
    Await.result(connection.connect, configuration.connectTimeout )

    connection
  }

  /**
   *
   * This method should "close" and release all resources acquired by the pooled object. This object will not be used
   * anymore so any cleanup necessary to remove it from memory should be made in this method. Implementors should not
   * raise an exception under any circumstances, the factory should log and clean up the exception itself.
   *
   * @param item
   */
  def destroy(item: MySQLConnection) =
    Try(item.disconnect).fold(
      e => log.error("Failed to close the connection", e),
      _ => ()
    )

  /**
   *
   * Validates that an object can still be used for it's purpose. This method should test the object to make sure
   * it's still valid for clients to use. If you have a database connection, test if you are still connected, if you're
   * accessing a file system, make sure you can still see and change the file.
   *
   * You decide how fast this method should return and what it will test, you should usually do something that's fast
   * enough not to slow down the pool usage, since this call will be made whenever an object returns to the pool.
   *
   * If this object is not valid anymore, a [[scala.util.Failure]] should be returned, otherwise [[scala.util.Success]]
   * should be the result of this call.
   *
   * @param item an object produced by this pool
   * @return
   */
  def validate(item: MySQLConnection): Try[MySQLConnection] =
    item match {
      case _ if item.isTimeouted => Failure(new ConnectionTimeoutedException(item))
      case _ if !item.isConnected => Failure(new ConnectionNotConnectedException(item))
      case _ if item.lastException != null => Failure(item.lastException)
      case _ if item.isQuerying => Failure(new ConnectionStillRunningQueryException(item.count, false))
      case _ => Success(item)
    }

  /**
   *
   * Does a full test on the given object making sure it's still valid. Different than validate, that's called whenever
   * an object is given back to the pool and should usually be fast, this method will be called when objects are
   * idle to make sure they don't "timeout" or become stale in anyway.
   *
   * For convenience, this method defaults to call **validate** but you can implement it in a different way if you
   * would like to.
   *
   * @param item an object produced by this pool
   * @return
   */
  override def test(item: MySQLConnection): Try[MySQLConnection] =
    Try {
      Await.result(item.sendQuery("SELECT 0"), configuration.testTimeout)
      item
    }
}
