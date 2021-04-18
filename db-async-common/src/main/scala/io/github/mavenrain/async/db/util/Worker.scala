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

import java.util.concurrent.ExecutorService
import scala.concurrent.{ExecutionContextExecutorService, ExecutionContext}
import scala.util.Try

object Worker {
  val log = Log.get[Worker]

  def apply() : Worker = apply(ExecutorServiceUtils.newFixedPool(1, "db-async-worker"))

  def apply( executorService : ExecutorService ) : Worker =
    new Worker(ExecutionContext.fromExecutorService( executorService ))

}

class Worker( val executionContext : ExecutionContextExecutorService ) {

  import Worker.log

  def action(f: => Unit): Unit =
    executionContext.execute(new Runnable {
      def run() = Try(f).fold(
        e => log.error("Failed to execute task %s".format(f), e),
        _ => ()
      )
    })

  def shutdown = executionContext.shutdown()

}
