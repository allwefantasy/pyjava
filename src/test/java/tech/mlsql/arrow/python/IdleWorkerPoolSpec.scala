package tech.mlsql.arrow.python

import java.net.{InetAddress, ServerSocket, Socket}
import org.scalatest.funsuite.AnyFunSuite

class IdleWorkerPoolSpec extends AnyFunSuite {
  private def withSockets(test: (Socket, Socket) => Unit): Unit = {
    val server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    val client = new Socket(server.getInetAddress, server.getLocalPort)
    val peer = server.accept()
    try test(client, peer)
    finally { client.close(); peer.close(); server.close() }
  }

  test("healthy socket is reused exclusively and its read timeout is restored") {
    withSockets { (socket, _) =>
      socket.setSoTimeout(1234)
      val pool = new IdleWorkerPool(2, Long.MaxValue, 1, _.close())
      pool.release(socket)
      pool.release(socket)
      assert(pool.size == 1)
      assert(pool.borrow().contains(socket))
      assert(socket.getSoTimeout == 1234)
      assert(pool.borrow().isEmpty)
    }
  }

  test("remote EOF and reset are discarded before reuse") {
    for (reset <- Seq(false, true)) {
      withSockets { (socket, peer) =>
        val pool = new IdleWorkerPool(2, Long.MaxValue, 50, _.close())
        pool.release(socket)
        if (reset) peer.setSoLinger(true, 0)
        peer.close()
        assert(pool.borrow().isEmpty)
        assert(socket.isClosed)
      }
    }
  }

  test("unread protocol bytes cannot enter the next task") {
    withSockets { (socket, peer) =>
      val pool = new IdleWorkerPool(2, Long.MaxValue, 50, _.close())
      pool.release(socket)
      peer.getOutputStream.write(1)
      peer.getOutputStream.flush()
      assert(pool.borrow().isEmpty)
      assert(socket.isClosed)
    }
  }

  test("idle expiration is per socket rather than refreshed by other returns") {
    var now = 0L
    withSockets { (first, _) =>
      withSockets { (second, _) =>
        val pool = new IdleWorkerPool(2, 100L, 1, _.close(), () => now)
        pool.release(first)
        now = 60
        pool.release(second)
        now = 100
        pool.evictExpired()
        assert(first.isClosed)
        assert(!second.isClosed)
        assert(pool.borrow().contains(second))
      }
    }
  }

  test("capacity overflow is closed, and zero disables idle retention") {
    withSockets { (first, _) =>
      withSockets { (second, _) =>
        val pool = new IdleWorkerPool(1, Long.MaxValue, 1, _.close())
        pool.release(first)
        pool.release(second)
        assert(second.isClosed)
        assert(pool.borrow().contains(first))
        val disabled = new IdleWorkerPool(0, Long.MaxValue, 1, _.close())
        disabled.release(first)
        assert(first.isClosed)
      }
    }
  }

  test("half-closed sockets are discarded and clear closes retained sockets") {
    withSockets { (first, _) =>
      withSockets { (second, _) =>
        val pool = new IdleWorkerPool(2, Long.MaxValue, 1, _.close())
        first.shutdownOutput()
        pool.release(first)
        assert(first.isClosed)
        pool.release(second)
        pool.clear()
        assert(second.isClosed)
        assert(pool.size == 0)
      }
    }
  }
}
