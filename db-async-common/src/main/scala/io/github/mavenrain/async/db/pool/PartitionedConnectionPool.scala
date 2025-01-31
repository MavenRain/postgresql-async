package io.github.mavenrain.async.db.pool;

import io.github.mavenrain.async.db.util.ExecutorServiceUtils
import io.github.mavenrain.async.db.{ QueryResult, Connection }
import scala.concurrent.{ ExecutionContext, Future }

class PartitionedConnectionPool[T <: Connection](
    factory: ObjectFactory[T],
    configuration: PoolConfiguration,
    numberOfPartitions: Int,
    executionContext: ExecutionContext = ExecutorServiceUtils.CachedExecutionContext)
    extends PartitionedAsyncObjectPool[T](factory, configuration, numberOfPartitions)
    with Connection {

    def disconnect: Future[Connection] = if (isConnected) {
        close.map(item => this)(executionContext)
    } else {
        Future.successful(this)
    }

    def connect: Future[Connection] = Future.successful(this)

    def isConnected: Boolean = !isClosed

    def sendQuery(query: String): Future[QueryResult] =
        use(_.sendQuery(query))(executionContext)

    def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] =
        use(_.sendPreparedStatement(query, values))(executionContext)

    override def inTransaction[A](f: Connection => Future[A])(implicit context: ExecutionContext = executionContext): Future[A] =
        use(_.inTransaction[A](f)(context))(executionContext)
}
