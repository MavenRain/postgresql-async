package io.github.mavenrain.async.db.mysql.message.client

import io.github.mavenrain.async.db.mysql.message.server.AuthenticationSwitchRequest

case class AuthenticationSwitchResponse( password : Option[String], request : AuthenticationSwitchRequest )
  extends ClientMessage(ClientMessage.AuthSwitchResponse)