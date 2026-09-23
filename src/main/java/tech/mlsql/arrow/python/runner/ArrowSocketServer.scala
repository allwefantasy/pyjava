package tech.mlsql.arrow.python.runner

import org.apache.spark.SparkException
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.log.Logging

import java.io.{BufferedOutputStream, IOException, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket, SocketTimeoutException}
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

/**
 * One-shot TCP server for a Spark partition.
 *
 * The returned [[ServerSocket]] is closed after the single reader finishes, fails, or the
 * task is cancelled. [[ArrowServerSocket.isClosed]] then throws if the partition was not
 * fully written, so the task waiting on `isClosed` cannot report success for a dropped partition.
 *
 * Wire bytes are unchanged: legacy Arrow IPC (no continuation token) plus the existing
 * length-prefixed control words on the Python-to-JVM read path.
 */
object ArrowSocketServer extends Logging {

  def serve(
             host: String,
             threadName: String,
             conf: Map[String, String],
             context: Option[CommonTaskContext],
             onClose: () => Unit = () => ())(writeFunc: OutputStream => Unit): Array[Any] = {
    val bufferSize = ArrowSockets.bufferSize(conf)
    val acceptTimeout = ArrowSockets.acceptTimeout(conf)
    val server = new ArrowServerSocket()
    server.setReuseAddress(true)
    server.bind(new InetSocketAddress(InetAddress.getByName(host), 0), 8)
    server.setSoTimeout(acceptTimeout)

    val clientRef = new AtomicReference[Socket](null)
    context.foreach { ctx =>
      ctx.javaSideSocketServerRegister()(server)
      ctx.readerRegister(() => ArrowSockets.closeQuietly(clientRef.get()))(null, null)
      if (ctx.isTaskCompleteOrInterrupt()()) {
        server.close()
      }
    }

    val thread = new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        var client: Socket = null
        var guard: java.util.concurrent.ScheduledFuture[_] = null
        try {
          client = server.accept()
          clientRef.set(client)
          ArrowSockets.prepare(client, bufferSize)
          val activity = new java.util.concurrent.atomic.AtomicLong(System.nanoTime())
          guard = ArrowSockets.watch(client, context,
            conf.getOrElse("python.socket.write.timeout", "300000").toInt, activity)
          if (context.exists(_.isTaskCompleteOrInterrupt()())) throw new IOException("Task ended before Arrow transfer")
          context.foreach(_.setTaskContext()())
          val tracked = new java.io.FilterOutputStream(client.getOutputStream) {
            override def write(b: Int): Unit = { out.write(b); activity.set(System.nanoTime()) }
            override def write(b: Array[Byte], off: Int, len: Int): Unit = {
              var offset = off
              while (offset < off + len) {
                val size = math.min(65536, off + len - offset)
                out.write(b, offset, size)
                activity.set(System.nanoTime())
                offset += size
              }
            }
          }
          val out = new BufferedOutputStream(tracked, bufferSize)
          try {
            writeFunc(out)
            out.flush()
          } catch {
            case NonFatal(e) =>
              if (!cancelledServe(server, context)) {
                server.fail(e)
                val reason = if (e.isInstanceOf[SocketTimeoutException]) {
                  "no reader connected before python.socket.accept.timeout"
                } else {
                  "failed while writing the Spark partition"
                }
                logError(s"Arrow socket serve $reason", e)
              }
          } finally {
            // Peer close after a successful flush is a normal end, not a failed partition.
            try out.close()
            catch {
              case NonFatal(_) =>
            }
          }
        } catch {
          case NonFatal(e) =>
            if (!cancelledServe(server, context)) {
              server.fail(e)
              val reason = if (e.isInstanceOf[SocketTimeoutException]) {
                "no reader connected before python.socket.accept.timeout"
              } else {
                "failed before the Spark partition was written"
              }
              logError(s"Arrow socket serve $reason", e)
            }
        } finally {
          if (guard != null) guard.cancel(false)
          try server.close()
          finally {
            ArrowSockets.closeQuietly(client)
            onClose()
          }
        }
      }
    }
    thread.start()
    Array(server, host, Int.box(server.getLocalPort))
  }

  private def cancelledServe(server: ArrowServerSocket, context: Option[CommonTaskContext]): Boolean = {
    server.closedQuietly || context.exists(_.isTaskCompleteOrInterrupt()())
  }
}

/**
 * Server socket whose `isClosed` reports a failed serve instead of looking like a normal end.
 * `close` itself must not throw that failure: `ServerSocket.close` calls `isClosed` internally.
 */
final class ArrowServerSocket extends ServerSocket {
  private val failure = new AtomicReference[Throwable](null)
  private val reportFailure = new java.util.concurrent.atomic.AtomicBoolean(false)
  @volatile private var inClose = false

  def fail(error: Throwable): Unit = {
    if (error != null) failure.compareAndSet(null, error)
  }

  /** Closed state that does not surface a serve failure. Used by the serve thread itself. */
  def closedQuietly: Boolean = super.isClosed

  override def close(): Unit = {
    if (inClose) {
      if (!super.isClosed) super.close()
      return
    }
    inClose = true
    try {
      if (!super.isClosed) super.close()
    } finally {
      reportFailure.set(true)
      inClose = false
    }
  }

  override def isClosed: Boolean = {
    val closed = super.isClosed
    if (!inClose && reportFailure.get) {
      val error = failure.get
      if (error != null) {
        throw new SparkException("Spark Arrow socket serve failed: " + error.getMessage, error)
      }
    }
    closed
  }
}

object ArrowSockets {
  val BufferKey = "python.socket.buffer"
  val AcceptTimeoutKey = "python.socket.accept.timeout"
  val DefaultBuffer = 1024 * 1024
  val DefaultAcceptTimeoutMs = 5 * 60 * 1000
  private val MaxBuffer = 64 * 1024 * 1024

  def bufferSize(conf: Map[String, String]): Int = {
    val raw = conf.get(BufferKey).orElse(conf.get("buffer_size")).getOrElse(DefaultBuffer.toString).toInt
    require(raw > 0 && raw <= MaxBuffer, s"socket buffer must be in 1..$MaxBuffer bytes, got $raw")
    raw
  }

  def acceptTimeout(conf: Map[String, String]): Int = {
    val raw = conf.getOrElse(AcceptTimeoutKey, DefaultAcceptTimeoutMs.toString).toInt
    require(raw > 0, s"$AcceptTimeoutKey must be positive")
    raw
  }

  def prepare(socket: Socket, bufferSize: Int): Unit = {
    socket.setTcpNoDelay(true)
    socket.setKeepAlive(true)
    socket.setReceiveBufferSize(bufferSize)
    socket.setSendBufferSize(bufferSize)
    socket.setPerformancePreferences(0, 1, 2)
  }

  private val monitor = new java.util.concurrent.ScheduledThreadPoolExecutor(1,
    new java.util.concurrent.ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "pyjava-arrow-socket-monitor")
        t.setDaemon(true)
        t
      }
    })
  monitor.setRemoveOnCancelPolicy(true)

  def watch(socket: Socket, context: Option[CommonTaskContext], timeoutMs: Int,
            activity: java.util.concurrent.atomic.AtomicLong): java.util.concurrent.ScheduledFuture[_] = {
    require(timeoutMs >= 0)
    monitor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        val expired = timeoutMs > 0 && System.nanoTime() - activity.get() > timeoutMs.toLong * 1000000
        if (expired || context.exists(_.isTaskCompleteOrInterrupt()())) closeQuietly(socket)
      }
    }, 100, 100, java.util.concurrent.TimeUnit.MILLISECONDS)
  }

  def closeQuietly(socket: Socket): Unit = {
    if (socket != null) {
      try socket.close()
      catch {
        case NonFatal(_) =>
      }
    }
  }
}
