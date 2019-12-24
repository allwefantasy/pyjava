package tech.mlsql.arrow.python.runner

import java.io.{BufferedOutputStream, OutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.{ArrowBatchStreamWriter, ArrowConverters, Utils}
import tech.mlsql.common.utils.distribute.socket.server.SocketServerInExecutor


class SparkSocketRunner(runnerName: String, host: String, timeZoneId: String) {

  def serveToStream(threadName: String)(writeFunc: OutputStream => Unit): Array[Any] = {
    val (_server, _host, _port) = SocketServerInExecutor.setupOneConnectionServer(host, runnerName)(s => {
      val out = new BufferedOutputStream(s.getOutputStream())
      Utils.tryWithSafeFinally {
        writeFunc(out)
      } {
        out.close()
      }
    })

    Array(_server, _host, _port)
  }

  def serveToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType, maxRecordsPerBatch: Int, context: CommonTaskContext) = {
    serveToStream(runnerName) { out =>
      val batchWriter = new ArrowBatchStreamWriter(schema, out, timeZoneId)
      val arrowBatch = ArrowConverters.toBatchIterator(
        iter, schema, maxRecordsPerBatch, timeZoneId, context)
      batchWriter.writeBatches(arrowBatch)
      batchWriter.end()
    }
  }

}
