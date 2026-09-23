package tech.mlsql.arrow.python

import java.net.{Socket, SocketTimeoutException}
import scala.collection.mutable
import scala.util.control.NonFatal

/** Task-boundary socket reuse; all methods run under the owning factory's lock. */
private[python] class IdleWorkerPool(
    maxSize: Int,
    idleTimeoutNanos: Long,
    validationTimeoutMs: Int,
    discard: Socket => Unit,
    nanoTime: () => Long = () => System.nanoTime()) {
  require(maxSize >= 0, "python.worker.pool.maxIdle must be non-negative")
  require(idleTimeoutNanos > 0, "python.worker.idle.time must be positive")
  require(validationTimeoutMs > 0, "python.worker.validate.timeout must be positive")

  private val idle = new mutable.LinkedHashMap[Socket, Long]()
  def size: Int = idle.size

  def release(socket: Socket): Unit = {
    // Duplicate returns must not lease one connection to two tasks.
    if (!idle.contains(socket)) {
      evictExpired()
      if (idle.size >= maxSize || !locallyOpen(socket)) discard(socket)
      else idle.put(socket, nanoTime())
    }
  }

  def borrow(): Option[Socket] = {
    evictExpired()
    while (idle.nonEmpty) {
      val socket = idle.head._1
      idle.remove(socket)
      if (healthy(socket)) return Some(socket)
      discard(socket)
    }
    None
  }

  def remove(socket: Socket): Unit = { idle.remove(socket) }

  def evictExpired(): Unit = {
    val now = nanoTime()
    idle.iterator.collect {
      case (socket, since) if now - since >= idleTimeoutNanos => socket
    }.toList.foreach { socket =>
      idle.remove(socket)
      discard(socket)
    }
  }

  def clear(): Unit = {
    val sockets = idle.keys.toList
    idle.clear()
    sockets.foreach(discard)
  }

  private def locallyOpen(socket: Socket): Boolean =
    socket.isConnected && !socket.isClosed && !socket.isInputShutdown && !socket.isOutputShutdown

  private def healthy(socket: Socket): Boolean = {
    if (!locallyOpen(socket)) return false
    val previousTimeout = socket.getSoTimeout
    try {
      // An idle worker sends no bytes. EOF, reset or leftover protocol data
      // means it cannot be reused. Sending a ping would break older workers.
      socket.setSoTimeout(validationTimeoutMs)
      socket.getInputStream.read()
      false
    } catch {
      case _: SocketTimeoutException => true
      case NonFatal(_) => false
    } finally {
      if (!socket.isClosed) {
        try socket.setSoTimeout(previousTimeout)
        catch { case NonFatal(_) => () }
      }
    }
  }
}
