package io.github.mavenrain.async.db.mysql.message.client

object QuitMessage {
  val Instance = new QuitMessage();
}

class QuitMessage extends ClientMessage( ClientMessage.Quit )