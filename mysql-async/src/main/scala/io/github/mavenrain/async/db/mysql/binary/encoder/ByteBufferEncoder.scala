package io.github.mavenrain.async.db.mysql.binary.encoder

import java.nio.ByteBuffer

import io.github.mavenrain.async.db.mysql.column.ColumnTypes
import io.github.mavenrain.async.db.util.ChannelWrapper.bufferToWrapper
import io.netty.buffer.ByteBuf

object ByteBufferEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    val bytes = value.asInstanceOf[ByteBuffer]

    buffer.writeLength(bytes.remaining())
    buffer.writeBytes(bytes)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_BLOB

}
