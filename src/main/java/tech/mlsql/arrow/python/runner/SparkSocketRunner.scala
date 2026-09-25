package tech.mlsql.arrow.python.runner

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.{SparkException, TaskContext, TaskKilledException}
import org.apache.spark.util.TaskFailureListener
import tech.mlsql.arrow._
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.log.Logging

import java.io._
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
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

  /**
   * Materialize the partition on the producing task into a token-scoped snapshot
   * on this process's shared listener, then return its descriptor. The descriptor
   * stays readable after this task completes; a failed or cancelled attempt marks
   * the snapshot failed and frees its resources instead of keeping a dead address.
   */
  def exportToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType,
                              maxRecordsPerBatch: Int, context: CommonTaskContext): PartitionDescriptor =
    exportToStreamWithArrow(iter, schema, maxRecordsPerBatch, context, Map.empty)

  def exportToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType,
                              maxRecordsPerBatch: Int, context: CommonTaskContext,
                              conf: Map[String, String]): PartitionDescriptor = {
    val service = ArrowSnapshotServices.get(host, conf)
    val (partitionId, attemptId) = context.innerContext match {
      case task: TaskContext => (task.partitionId().toLong, task.taskAttemptId())
      case _ => (-1L, -1L)
    }
    val entry = service.register(partitionId, attemptId, conf)
    // Success completion must not drop a committed snapshot. Only an attempt
    // that is still preparing is failed here. A failure after commit is handled
    // by the task-failure listener below, which does clean it up.
    context.readerRegister(() => {
      entry.failIfPreparing("producer task ended before the partition snapshot was ready")
      service.settleQuiet(entry)
    })(null, null)
    context.innerContext match {
      case task: TaskContext =>
        task.addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            val message = Option(error).flatMap(e => Option(e.getMessage)).getOrElse("producer task failed")
            entry.fail("producer task failed: " + message)
            service.settleQuiet(entry)
          }
        })
      case _ =>
    }
    val bytes = conf.getOrElse("python.arrow.maxBytesPerBatch", "8388608").toLong
    val allocation = conf.getOrElse("python.arrow.maxAllocation", "134217728").toLong
    val bufferSize = ArrowSockets.bufferSize(conf)
    var path: java.nio.file.Path = null
    try {
      entry.beginWrite()
      path = service.newSpoolFile(conf)
      entry.attachFile(path)
      val out = new BufferedOutputStream(new FileOutputStream(path.toFile), bufferSize)
      val metered = new FilterOutputStream(out) {
        override def write(b: Int): Unit = write(Array(b.toByte), 0, 1)
        override def write(b: Array[Byte], off: Int, len: Int): Unit = {
          entry.noteWrite(len, service.charge)
          out.write(b, off, len)
        }
      }
      try {
        ArrowConverters.writeLegacyArrowStream(iter, schema, maxRecordsPerBatch,
          timeZoneId, metered, context, bytes, allocation)
      } finally metered.close()
      entry.markReady(path, entry.chargedBytes)
      PartitionDescriptor(service.advertiseHost, service.port, entry.token,
        SnapshotProtocol.ProtocolName, partitionId, attemptId, entry.chargedBytes,
        entry.leaseDeadlineMs, timeZoneId)
    } catch {
      case scala.util.control.NonFatal(e) =>
        entry.abortWrite(Option(e.getMessage).getOrElse(e.getClass.getName))
        service.settleQuiet(entry)
        throw e
    }
  }

  /**
   * Read a shared snapshot into InternalRows. STATUS is polled on the same
   * connection until ready (bounded by python.socket.shared.wait.ready.ms); a
   * failed generation reports the server error instead of a network timeout.
   */
  def readFromSharedSnapshot(host: String, port: Int, token: String,
                             context: CommonTaskContext): Iterator[InternalRow] =
    readFromSharedSnapshot(host, port, token, context, Map.empty)

  def readFromSharedSnapshot(host: String, port: Int, token: String,
                             context: CommonTaskContext,
                             conf: Map[String, String]): Iterator[InternalRow] = {
    val connectTimeout = conf.getOrElse("python.connect.timeout", "10000").toInt
    val readTimeout = conf.getOrElse("python.socket.read.timeout", "300000").toInt
    val handshakeTimeout = conf.getOrElse("python.socket.shared.handshake.timeout", "10000").toInt
    val waitReadyMs = conf.getOrElse("python.socket.shared.wait.ready.ms", "1800000").toLong
    val pollMs = conf.getOrElse("python.socket.shared.status.poll.ms", "100").toLong
    require(connectTimeout > 0 && readTimeout >= 0 && handshakeTimeout > 0 &&
      waitReadyMs > 0 && pollMs > 0, "Invalid snapshot socket timeout")
    val bufferSize = ArrowSockets.bufferSize(conf)
    val maxAllocation = conf.getOrElse("python.arrow.maxAllocation", "134217728").toLong
    require(maxAllocation > 0, "python.arrow.maxAllocation must be positive")

    val socket = new Socket()
    val released = new AtomicBoolean(false)
    def closeSocket(): Unit = if (released.compareAndSet(false, true)) ArrowSockets.closeQuietly(socket)
    var handedOff = false
    // Registered before STATUS/READ so cancellation closes the socket instead
    // of leaving it blocked in a handshake. The iterator registers again once
    // it owns the connection; both closes are idempotent.
    try {
      ArrowSockets.prepare(socket, bufferSize)
      socket.connect(new InetSocketAddress(host, port), connectTimeout)
      socket.setSoTimeout(handshakeTimeout)
      context.readerRegister(() => closeSocket())(null, null)
      val activity = new AtomicLong(System.nanoTime())
      val guard = ArrowSockets.watch(socket, Some(context), handshakeTimeout, activity)
      try {
        val stream = new DataInputStream(
          new BufferedInputStream(socket.getInputStream, bufferSize))
        val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
        val deadline = System.currentTimeMillis() + waitReadyMs
        var ready = false
        while (!ready) {
          throwIfKilled(context)
          SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpStatus, token)
          out.flush()
          activity.set(System.nanoTime())
          val (status, info) = SnapshotProtocol.readResponse(stream)
          activity.set(System.nanoTime())
          status match {
            case SnapshotProtocol.StReady => ready = true
            case SnapshotProtocol.StPreparing =>
              val now = System.currentTimeMillis()
              if (now >= deadline) {
                throw new SparkException(s"shared snapshot still preparing after " +
                  s"${waitReadyMs}ms (phase=wait-ready)")
              }
              try Thread.sleep(math.min(pollMs, math.max(1L, deadline - now)))
              catch {
                case _: InterruptedException =>
                  throw new TaskKilledException(context.getTaskKillReason()().getOrElse("interrupted"))
              }
            case code =>
              throw new SparkException(s"shared snapshot ${SnapshotProtocol.statusName(code)} " +
                s"during phase=status: $info")
          }
        }
        throwIfKilled(context)
        SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpRead, token)
        out.flush()
        val (readStatus, readInfo) = SnapshotProtocol.readResponse(stream)
        if (readStatus != SnapshotProtocol.StReady) {
          throw new SparkException(s"shared snapshot ${SnapshotProtocol.statusName(readStatus)} " +
            s"during phase=read: $readInfo")
        }
        val length = stream.readLong()
        if (length < 0) throw new SparkException("invalid shared snapshot payload length")
        socket.setSoTimeout(readTimeout)
        val iterator = new SharedSnapshotIterator(socket, stream, length, context, readTimeout,
          maxAllocation, s"$host:$port")
        handedOff = true
        iterator
      } finally {
        guard.cancel(false)
      }
    } catch {
      case killed: TaskKilledException =>
        throw killed
      case killed: tech.mlsql.arrow.python.iapp.TaskKilledException =>
        throw killed
      case scala.util.control.NonFatal(e) =>
        if (context.isTaskInterrupt()()) {
          throw new TaskKilledException(context.getTaskKillReason()().getOrElse("interrupted"))
        } else throw e
    } finally {
      if (!handedOff) closeSocket()
    }
  }

  private def throwIfKilled(context: CommonTaskContext): Unit = {
    context.killTaskIfInterrupted()()
    if (Thread.currentThread().isInterrupted) {
      throw new TaskKilledException(context.getTaskKillReason()().getOrElse("interrupted"))
    }
  }

  /** Release a snapshot early; returns false when the token is already gone. */
  def releaseSharedSnapshot(host: String, port: Int, token: String): Boolean =
    releaseSharedSnapshot(host, port, token, Map.empty)

  def releaseSharedSnapshot(host: String, port: Int, token: String,
                            conf: Map[String, String]): Boolean = {
    snapshotControl(host, port, token, SnapshotProtocol.OpRelease, conf)._1 ==
      SnapshotProtocol.StReady
  }

  /** @return (status code, info JSON) for a token, without consuming the snapshot. */
  def sharedSnapshotStatus(host: String, port: Int, token: String,
                           conf: Map[String, String]): (Int, String) =
    snapshotControl(host, port, token, SnapshotProtocol.OpStatus, conf)

  private def snapshotControl(host: String, port: Int, token: String, op: Int,
                              conf: Map[String, String]): (Int, String) = {
    val connectTimeout = conf.getOrElse("python.connect.timeout", "10000").toInt
    val handshakeTimeout = conf.getOrElse("python.socket.shared.handshake.timeout", "10000").toInt
    val socket = new Socket()
    try {
      ArrowSockets.prepare(socket, ArrowSockets.bufferSize(conf))
      socket.connect(new InetSocketAddress(host, port), connectTimeout)
      socket.setSoTimeout(handshakeTimeout)
      val in = new DataInputStream(new BufferedInputStream(socket.getInputStream, 65536))
      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, 65536))
      SnapshotProtocol.writeRequest(out, op, token)
      out.flush()
      SnapshotProtocol.readResponse(in)
    } finally socket.close()
  }

}

/**
 * Iterator over one shared snapshot payload. The Arrow reader consumes exactly
 * the announced byte length; anything shorter fails instead of looking complete.
 */
final class SharedSnapshotIterator(
                                    socket: Socket,
                                    stream: DataInputStream,
                                    length: Long,
                                    context: CommonTaskContext,
                                    readTimeout: Int,
                                    maxAllocation: Long,
                                    endpoint: String)
  extends Iterator[InternalRow] with Logging {

  private val activity = new AtomicLong(System.nanoTime())
  private val counted = new BoundedSnapshotInput(stream, length, activity)
  private val resourceLock = new Object
  private var allocator: org.apache.arrow.memory.BufferAllocator = _
  private var guard: java.util.concurrent.ScheduledFuture[_] = _
  private var reader: ArrowStreamReader = _
  private var root: VectorSchemaRoot = _
  private var vectors: Array[ColumnVector] = _
  private var rowIter: Iterator[InternalRow] = Iterator.empty
  private var eos = false
  private val arrowClosed = new AtomicBoolean(false)

  private def closeArrow(): Unit = {
    if (arrowClosed.compareAndSet(false, true)) {
      try { if (reader != null) reader.close(false) }
      finally { if (allocator != null) allocator.close() }
    }
  }

  private def closeResources(): Unit = {
    if (guard != null) guard.cancel(false)
    ArrowSockets.closeQuietly(socket)
    resourceLock.synchronized(closeArrow())
  }

  try {
    allocator = ArrowUtils.rootAllocator.newChildAllocator(
      "shared snapshot reader", 0, maxAllocation)
    guard = ArrowSockets.watch(socket, Some(context), readTimeout, activity)
    context.readerRegister(() => closeResources())(null, null)
  } catch {
    case scala.util.control.NonFatal(e) =>
      closeResources()
      throw e
  }

  private def nextBatch(): Unit = resourceLock.synchronized {
    context.killTaskIfInterrupted()()
    if (reader == null) {
      try {
        reader = new ArrowStreamReader(counted, allocator)
        root = reader.getVectorSchemaRoot()
        vectors = root.getFieldVectors().asScala.map { vector =>
          new ArrowColumnVector(vector)
        }.toArray[ColumnVector]
      } catch {
        case scala.util.control.NonFatal(e) =>
          throw new SparkException(s"failed to open shared snapshot stream from $endpoint", e)
      }
    }
    if (reader.loadNextBatch()) {
      val batch = new ColumnarBatch(vectors)
      batch.setNumRows(root.getRowCount)
      rowIter = batch.rowIterator().asScala
    } else {
      counted.expectEnd()
      eos = true
      closeResources()
    }
  }

  override def hasNext: Boolean = {
    if (eos) return false
    try {
      while (!eos && !rowIter.hasNext) nextBatch()
      !eos
    } catch {
      case scala.util.control.NonFatal(e) =>
        try closeResources()
        catch {
          case scala.util.control.NonFatal(cleanup) => e.addSuppressed(cleanup)
        }
        throw e match {
          case killed: TaskKilledException => killed
          case spark: SparkException => spark
          case other => new SparkException(s"error reading shared snapshot from $endpoint", other)
        }
    }
  }

  override def next(): InternalRow = {
    if (hasNext) rowIter.next() else Iterator.empty.next()
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
