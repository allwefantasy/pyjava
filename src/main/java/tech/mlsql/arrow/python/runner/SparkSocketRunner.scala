package tech.mlsql.arrow.python.runner

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.{SparkException, TaskKilledException}
import tech.mlsql.arrow._
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.log.Logging

import java.io._
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._


class SparkSocketRunner(runnerName: String, host: String, timeZoneId: String) {

  def serveToStream(threadName: String)(writeFunc: OutputStream => Unit): Array[Any] =
    serveToStream(threadName, Map.empty[String, String])(writeFunc)

  def serveToStream(threadName: String, conf: Map[String, String])(writeFunc: OutputStream => Unit): Array[Any] =
    ArrowSocketServer.serve(host, threadName, conf, None)(writeFunc)

  def serveToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType, maxRecordsPerBatch: Int, context: CommonTaskContext): Array[Any] =
    serveToStreamWithArrow(iter, schema, maxRecordsPerBatch, context, Map.empty)

  def serveToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType, maxRecordsPerBatch: Int,
                             context: CommonTaskContext, conf: Map[String, String]): Array[Any] = {
    val bytes = conf.getOrElse("python.arrow.maxBytesPerBatch", "8388608").toLong
    val allocation = conf.getOrElse("python.arrow.maxAllocation", "134217728").toLong
    def write(out: OutputStream): Unit =
      ArrowConverters.writeLegacyArrowStream(iter, schema, maxRecordsPerBatch, timeZoneId,
        out, context, bytes, allocation)
    if (conf.getOrElse("python.socket.detached", "false").toBoolean) {
      // Consume the Spark iterator on its owning task before returning an address.
      // Only an immutable snapshot may outlive that task.
      val limit = conf.getOrElse("python.socket.spool.maxBytes", "1073741824").toLong
      require(limit > 0)
      val path = java.nio.file.Files.createTempFile("pyjava-partition-", ".arrow")
      try {
        val out = new BufferedOutputStream(new FileOutputStream(path.toFile), ArrowSockets.bufferSize(conf))
        val limited = new FilterOutputStream(out) {
          private var written = 0L
          override def write(b: Int): Unit = write(Array(b.toByte), 0, 1)
          override def write(b: Array[Byte], off: Int, len: Int): Unit = {
            if (written + len > limit) throw new IOException("Partition exceeds python.socket.spool.maxBytes")
            out.write(b, off, len)
            written += len
          }
        }
        try write(limited) finally limited.close()
        ArrowSocketServer.serve(host, runnerName, conf, None,
          () => java.nio.file.Files.deleteIfExists(path)) { stream =>
          java.nio.file.Files.copy(path, stream)
        }
      } catch {
        case scala.util.control.NonFatal(e) => java.nio.file.Files.deleteIfExists(path); throw e
      }
    } else {
      ArrowSocketServer.serve(host, runnerName, conf, Some(context))(write)
    }
  }

  def readFromStreamWithArrow(host: String, port: Int, context: CommonTaskContext): Iterator[InternalRow] =
    readFromStreamWithArrow(host, port, context, Map.empty)

  def readFromStreamWithArrow(host: String, port: Int, context: CommonTaskContext,
                             conf: Map[String, String]): Iterator[InternalRow] = {
    val connectTimeout = conf.getOrElse("python.connect.timeout", "10000").toInt
    val readTimeout = conf.getOrElse("python.socket.read.timeout", "300000").toInt
    require(connectTimeout > 0 && readTimeout >= 0, "Invalid socket timeout")
    val bufferSize = ArrowSockets.bufferSize(conf)
    val maxAllocation = conf.getOrElse("python.arrow.maxAllocation", "134217728").toLong
    require(maxAllocation > 0, "python.arrow.maxAllocation must be positive")
    val socket = new Socket()
    try {
      ArrowSockets.prepare(socket, bufferSize)
      socket.connect(new InetSocketAddress(host, port), connectTimeout)
      socket.setSoTimeout(readTimeout)
    } catch {
      case scala.util.control.NonFatal(e) => socket.close(); throw e
    }
    val activity = new java.util.concurrent.atomic.AtomicLong(System.nanoTime())
    val guard = ArrowSockets.watch(socket, Some(context), readTimeout, activity)
    val input = new FilterInputStream(socket.getInputStream) {
      override def read(): Int = { val n = in.read(); activity.set(System.nanoTime()); n }
      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        val n = in.read(b, off, len); activity.set(System.nanoTime()); n
      }
    }
    val stream = new DataInputStream(new BufferedInputStream(input, bufferSize))
    val outfile = new DataOutputStream(socket.getOutputStream)
    new ReaderIterator[ColumnarBatch](stream, System.currentTimeMillis(), context) {
      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader ", 0, maxAllocation)

      private val resourceLock = new Object
      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _
      private val arrowClosed = new AtomicBoolean(false)
      private def closeArrow(): Unit = {
        if (arrowClosed.compareAndSet(false, true)) {
          try { if (reader != null) reader.close(false) }
          finally { allocator.close() }
        }
      }
      private def closeResources(): Unit = {
        guard.cancel(false)
        socket.close() // unblock a concurrent read before acquiring the Arrow resource lock
        resourceLock.synchronized { closeArrow() }
      }
      context.readerRegister(() => closeResources())(null, null)

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = resourceLock.synchronized {
        try {
          context.killTaskIfInterrupted()()
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              closeArrow()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>

                try {
                  reader = new ArrowStreamReader(stream, allocator)
                  root = reader.getVectorSchemaRoot()
                  schema = ArrowUtils.fromArrowSchema(root.getSchema())
                  vectors = root.getFieldVectors().asScala.map { vector =>
                    new ArrowColumnVector(vector)
                  }.toArray[ColumnVector]
                  read()
                } catch {
                  case e: IOException if (e.getMessage.contains("Missing schema") || e.getMessage.contains("Expected schema but header was")) =>
                    logInfo("Arrow read schema fail", e)
                    if (reader != null) reader.close(false)
                    reader = null
                    read()
                }

              case SpecialLengths.ARROW_STREAM_CRASH =>
                read()

              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException(outfile)

              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection(outfile)
                closeResources()
                null

              case flag => throw new IOException(s"Invalid Arrow socket message: $flag")
            }
          }
        } catch {
          case scala.util.control.NonFatal(e) =>
            try closeResources()
            catch { case scala.util.control.NonFatal(cleanup) => e.addSuppressed(cleanup) }
            handleException(e)
        }
      }
    }.flatMap { batch =>
      batch.rowIterator.asScala
    }
  }

}

object SparkSocketRunner {

}

abstract class ReaderIterator[OUT](
                                    stream: DataInputStream,
                                    startTime: Long,
                                    context: CommonTaskContext)
  extends Iterator[OUT] with Logging {

  private var nextObj: OUT = _
  private var eos = false

  override def hasNext: Boolean = nextObj != null || {
    if (!eos) {
      nextObj = read()
      hasNext
    } else {
      false
    }
  }

  override def next(): OUT = {
    if (hasNext) {
      val obj = nextObj
      nextObj = null.asInstanceOf[OUT]
      obj
    } else {
      Iterator.empty.next()
    }
  }

  /**
   * Reads next object from the stream.
   * When the stream reaches end of data, needs to process the following sections,
   * and then returns null.
   */
  protected def read(): OUT


  protected def handlePythonException(out: DataOutputStream): SparkException = {
    // Signals that an exception has been thrown in python
    val exLength = stream.readInt()
    if (exLength < 0 || exLength > 1048576) throw new IOException("Invalid Python error frame length")
    val obj = new Array[Byte](exLength)
    stream.readFully(obj)
    try {
      out.writeInt(SpecialLengths.END_OF_STREAM)
      out.flush()
    } catch {
      case e: Exception => logError("", e)
    }
    new SparkException(new String(obj, StandardCharsets.UTF_8), null)
  }

  protected def handleEndOfStream(out: DataOutputStream): Unit = {

    eos = true
  }

  protected def handleEndOfDataSection(out: DataOutputStream): Unit = {
    //read end of stream
    val flag = stream.readInt()
    if (flag != SpecialLengths.END_OF_STREAM) {
      throw new IOException(s"Invalid Arrow socket end-of-stream marker: $flag")
    }
    out.writeInt(SpecialLengths.END_OF_STREAM)
    out.flush()
    eos = true
  }

  protected val handleException: PartialFunction[Throwable, OUT] = {
    case e: Exception if context.isTaskInterrupt()() =>
      logDebug("Exception thrown after task interruption", e)
      throw new TaskKilledException(context.getTaskKillReason()().getOrElse("unknown reason"))
    case e: SparkException =>
      throw e
    case eof: EOFException =>
      throw new SparkException("Python worker exited unexpectedly (crashed)", eof)

    case e: Exception =>
      throw new SparkException("Error to read", e)
  }
}
