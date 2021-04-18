package io.github.mavenrain.async.db.exceptions

import io.github.mavenrain.async.db.Connection

class ConnectionTimeoutedException( val connection : Connection )
  extends DatabaseException( "The connection %s has a timeouted query and is being closed".format(connection) )