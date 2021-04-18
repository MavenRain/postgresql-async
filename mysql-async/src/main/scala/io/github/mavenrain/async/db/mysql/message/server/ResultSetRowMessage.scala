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

import io.netty.buffer.ByteBuf
import scala.collection.IterableOnce
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.chaining.scalaUtilChainingOps

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

  def insert(idx: Int, elem: ByteBuf): Unit =
    buffer.insert(idx, elem)
    
  def insertAll(idx: Int, elems: IterableOnce[ByteBuf]): Unit =
    buffer.insertAll(idx, elems)
    
  def patchInPlace(from: Int, patch: IterableOnce[ByteBuf], replaced: Int): this.type =
    buffer.patchInPlace(from, patch, replaced).pipe(_ => this)
  def prepend(elem: ByteBuf): this.type =
    buffer.prepend(elem).pipe(_ => this)
  def remove(idx: Int, count: Int): Unit =
    buffer.remove(idx, count)

  def insertAll(n: Int, elems: Iterable[ByteBuf]) =
    buffer.insertAll(n, elems)

  def remove(n: Int): ByteBuf =
    buffer.remove(n)

  override def iterator: Iterator[ByteBuf] = buffer.iterator

}