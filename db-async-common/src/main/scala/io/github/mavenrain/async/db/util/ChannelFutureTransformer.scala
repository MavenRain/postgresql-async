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

import io.netty.channel.{ChannelFutureListener, ChannelFuture}
import scala.concurrent.{Promise, Future}
import io.github.mavenrain.async.db.exceptions.CanceledChannelFutureException
import scala.language.implicitConversions

object ChannelFutureTransformer {

  implicit def toFuture(channelFuture: ChannelFuture): Future[ChannelFuture] = {
    val promise = Promise[ChannelFuture]()

    channelFuture.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture): Unit =
        future match {
          case _ if future.isSuccess => promise.success(future)
          case _ if future.cause == null => promise.failure(
            new CanceledChannelFutureException(future)
                .fillInStackTrace()
          )
          case _ => promise.failure(future.cause)
        }
    })

    promise.future
  }

}
