package io.github.mavenrain.async.db.exceptions

import io.github.mavenrain.async.db.Connection

class ConnectionNotConnectedException( val connection : Connection )
  extends DatabaseException( "The connection %s is not connected to the database".format(connection) )