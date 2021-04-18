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

package io.github.mavenrain.async.db.mysql.message.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import io.netty.buffer.ByteBuf

class ResultSetRowMessage
  extends ServerMessage( ServerMessage.Row )
  with mutable.Buffer[ByteBuf]
{

  private val buffer = new ArrayBuffer[ByteBuf]()

  def length: Int = buffer.length

  def apply(idx: Int): ByteBuf = buffer(idx)

  def update(n: Int, newelem: ByteBuf) =
    buffer.update(n, newelem)

  def addOne(elem: ByteBuf): this.type = {
    buffer += elem
    this
  }

  def clear() = buffer.clear()

  def insert(idx: Int, elem: io.netty.buffer.ByteBuf): Unit =
    take(idx) :+ elem +: drop(idx)
  def insertAll(idx: Int, elems: scala.collection.IterableOnce[io.netty.buffer.ByteBuf]): Unit =
    take(idx) ++ elems ++ drop(idx)
  def patchInPlace(from: Int, patch: scala.collection.IterableOnce[io.netty.buffer.ByteBuf], replaced: Int): this.type = this
  def prepend(elem: io.netty.buffer.ByteBuf): this.type = this
  def remove(idx: Int, count: Int): Unit =
    take(idx) ++ drop(idx + 1)

  def insertAll(n: Int, elems: Iterable[ByteBuf]) =
    buffer.insertAll(n, elems)

  def remove(n: Int): ByteBuf =
    buffer.remove(n)

  override def iterator: Iterator[ByteBuf] = buffer.iterator

}