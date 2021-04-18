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

package io.github.mavenrain.async.db.pool

import io.github.mavenrain.async.db.util.{Log, Worker}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicLong
import java.util.{Timer, TimerTask}
import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import scala.util.chaining.scalaUtilChainingOps

object SingleThreadedAsyncObjectPool {
  val Counter = new AtomicLong()
  val log = Log.get[SingleThreadedAsyncObjectPool[Nothing]]
}

/**
 *
 * Implements an [[io.github.mavenrain.async.db.pool.AsyncObjectPool]] using a single thread from a
 * fixed executor service as an event loop to cause all calls to be sequential.
 *
 * Once you are done with this object remember to call it's close method to clean up the thread pool and
 * it's objects as this might prevent your application from ending.
 *
 * @param factory
 * @param configuration
 * @tparam T type of the object this pool holds
 */

class SingleThreadedAsyncObjectPool[T](
                                        factory: ObjectFactory[T],
                                        configuration: PoolConfiguration
                                        ) extends AsyncObjectPool[T] {

  import SingleThreadedAsyncObjectPool.{Counter, log}

  private val mainPool = Worker()
  private var poolables = List.empty[PoolableHolder[T]]
  private val checkouts = new ArrayBuffer[T](configuration.maxObjects)
  private val waitQueue = new Queue[Promise[T]]()
  private val timer = new Timer("async-object-pool-timer-" + Counter.incrementAndGet(), true)
  timer.scheduleAtFixedRate(new TimerTask {
    def run(): Unit =
      mainPool.action {
        testObjects
      }
  }, configuration.validationInterval, configuration.validationInterval)

  private var closed = false

  /**
   *
   * Asks for an object from the pool, this object should be returned to the pool when not in use anymore.
   *
   * @return
   */

  def take: Future[T] = {

    if (closed) {
      return Promise.failed(new PoolAlreadyTerminatedException).future
    }

    val promise = Promise[T]()
    checkout(promise)
    promise.future
  }

  /**
   *
   * Returns an object to the pool. The object is validated before being added to the collection
   * of available objects to make sure we have a usable object. If the object isn't valid it's discarded.
   *
   * @param item
   * @return
   */

  def giveBack(item: T): Future[AsyncObjectPool[T]] = {
    val promise = Promise[AsyncObjectPool[T]]()
    mainPool.action {
      // Ensure it came from this pool
      val idx = checkouts.indexOf(item)
      if(idx >= 0) {
        checkouts.remove(idx)
        factory.validate(item) match {
          case Success(item) => {
            addBack(item, promise)
          }
          case Failure(e) => {
            factory.destroy(item)
            promise.failure(e)
          }
        }
      } else {
        // It's already a failure but lets doublecheck why
        val isFromOurPool = (item match {
          case x: AnyRef => poolables.find(holder => x eq holder.item.asInstanceOf[AnyRef])
          case _ => poolables.find(holder => item == holder.item)
        }).isDefined

        if(isFromOurPool) {
          promise.failure(new IllegalStateException("This item has already been returned"))
        } else {
          promise.failure(new IllegalArgumentException("The returned item did not come from this pool."))
        }
      }
    }

    promise.future
  }

  def isFull: Boolean = poolables.isEmpty && checkouts.size == configuration.maxObjects

  def close: Future[AsyncObjectPool[T]] = {
    try {
      val promise = Promise[AsyncObjectPool[T]]()
      mainPool.action {
        if (!closed) {
          try {
            timer.cancel()
            mainPool.shutdown
            closed = true
            (poolables.map(i => i.item) ++ checkouts).foreach(item => factory.destroy(item))
            promise.success(this)
          } catch {
            case e: Exception => promise.failure(e)
          }
        } else {
          promise.success(this)
        }
      }
      promise.future
    } catch {
      case e: RejectedExecutionException if closed =>
        Future.successful(this)
    }
  }

  def availables: Iterable[T] = poolables.map(item => item.item)

  def inUse: Iterable[T] = checkouts

  def queued: Iterable[Promise[T]] = waitQueue

  def isClosed: Boolean = closed

  /**
   *
   * Adds back an object that was in use to the list of poolable objects.
   *
   * @param item
   * @param promise
   */

  private def addBack(item: T, promise: Promise[AsyncObjectPool[T]]): Unit = {
    poolables ::= new PoolableHolder[T](item)
    if (waitQueue.nonEmpty) checkout(waitQueue.dequeue())
    promise.success(this)
  }

  /**
   *
   * Enqueues a promise to be fulfilled in the future when objects are sent back to the pool. If
   * we have already reached the limit of enqueued objects, fail the promise.
   *
   * @param promise
   */

  private def enqueuePromise(promise: Promise[T]): Unit =
    if (waitQueue.size >= configuration.maxQueueSize) {
      val exception = new PoolExhaustedException("There are no objects available and the waitQueue is full")
      exception.fillInStackTrace()
      promise.failure(exception)
    } else
      waitQueue += promise

  private def checkout(promise: Promise[T]): Unit =
    mainPool.action {
      if (isFull)
        enqueuePromise(promise)
      else
        createOrReturnItem(promise)
    }

  /**
   *
   * Checks if there is a poolable object available and returns it to the promise.
   * If there are no objects available, create a new one using the factory and return it.
   *
   * @param promise
   */

  private def createOrReturnItem(promise: Promise[T]): Unit =
    if (poolables.isEmpty) {
      try {
        val item = factory.create
        checkouts += item
        promise.success(item)
      } catch {
        case e: Exception => promise.failure(e)
      }
    } else {
      val h :: t = poolables
      poolables = t
      val item = h.item
      checkouts += item
      promise.success(item)
    }

  override def finalize() = close

  /**
   *
   * Validates pooled objects not in use to make sure they are all usable, great if
   * you're holding onto network connections since you can "ping" the destination
   * to keep the connection alive.
   *
   */

  private def testObjects: Unit =
    new ArrayBuffer[PoolableHolder[T]]()
      .tap(removals =>
        poolables.foreach {
          poolable =>
            factory.test(poolable.item) match {
              case Success(item) => {
                if (poolable.timeElapsed > configuration.maxIdle) {
                  log.debug("Connection was idle for {}, maxIdle is {}, removing it", poolable.timeElapsed, configuration.maxIdle)
                  removals += poolable
                  factory.destroy(poolable.item)
                }
              }
              case Failure(e) => {
                log.error("Failed to validate object", e)
                removals += poolable
                factory.destroy(poolable.item)
              }
            }
        }
      )
      .tap(removals => poolables = poolables.diff(removals))

  private class PoolableHolder[T](val item: T) {
    val time = System.currentTimeMillis()

    def timeElapsed = System.currentTimeMillis() - time
  }

}
