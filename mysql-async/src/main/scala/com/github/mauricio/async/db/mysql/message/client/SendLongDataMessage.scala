package io.github.mavenrain.async.db.mysql.message.client

import io.netty.buffer.ByteBuf

case class SendLongDataMessage (
                                 statementId : Array[Byte],
                                 value : ByteBuf,
                                 paramId : Int )