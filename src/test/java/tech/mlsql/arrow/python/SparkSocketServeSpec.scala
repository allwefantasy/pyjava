package tech.mlsql.arrow.python

import java.io.{BufferedInputStream, ByteArrayOutputStream}
import java.net.{InetSocketAddress, Socket}
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.SparkException
import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowSockets, SparkSocketRunner}
import tech.mlsql.arrow.{ArrowBatchStreamWriter, ArrowConverters, ArrowUtils}

class SparkSocketServeSpec extends AnyFunSuite {

  private val schema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true)))

  private def rows(values: Seq[(Long, String)]): Iterator[org.apache.spark.sql.catalyst.InternalRow] = {
    val encoder = WowRowEncoder.fromRow(schema)
    values.iterator.map { case (id, name) => encoder(Row(id, name)).copy() }
  }

  private def legacyBytes(values: Seq[(Long, String)], batch: Int): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val context = new AppContextImpl(new JavaContext, null)
    val writer = new ArrowBatchStreamWriter(schema, out, "UTC")
    writer.writeBatches(ArrowConverters.toBatchIterator(rows(values), schema, batch, "UTC", context))
    writer.end()
    out.toByteArray
  }

  private def directBytes(values: Seq[(Long, String)], batch: Int): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val javaContext = new JavaContext
    val context = new AppContextImpl(javaContext, null)
    ArrowConverters.writeLegacyArrowStream(rows(values), schema, batch, "UTC", out, context)
    javaContext.close
    out.toByteArray
  }

  private def portOf(result: Array[Any]): Int = result(2).asInstanceOf[Number].intValue

  private def waitClosed(server: java.net.ServerSocket, timeoutMs: Long): Boolean = {
    val deadline = System.currentTimeMillis + timeoutMs
    var closed = false
    while (!closed && System.currentTimeMillis < deadline) {
      closed = server.isClosed
      if (!closed) Thread.sleep(10)
    }
    closed
  }

  test("direct Arrow socket bytes match the legacy batch writer") {
    val values = Seq[(Long, String)]((1L, "a"), (2L, null), (3L, "c"), (4L, "d"))
    assert(directBytes(values, 2).sameElements(legacyBytes(values, 2)))
    assert(directBytes(Seq.empty, 2).sameElements(legacyBytes(Seq.empty, 2)))
  }

  test("served partition is readable and releases Arrow memory") {
    val before = ArrowUtils.rootAllocator.getAllocatedMemory
    val ctx = new JavaContext
    val app = new AppContextImpl(ctx, null)
    val runner = new SparkSocketRunner("serve", "127.0.0.1", "UTC")
    val encoder = WowRowEncoder.fromRow(StructType(Seq(StructField("value", LongType, nullable = false))))
    val iter = (0 until 10000).iterator.map(i => encoder(Row(i.toLong)).copy())
    val valueSchema = StructType(Seq(StructField("value", LongType, nullable = false)))
    var socket: Socket = null
    var reader: ArrowStreamReader = null
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("serve-spec", 0, Long.MaxValue)
    try {
      val result = runner.serveToStreamWithArrow(iter, valueSchema, 1000, app)
      socket = new Socket()
      socket.connect(new InetSocketAddress("127.0.0.1", portOf(result)), 5000)
      reader = new ArrowStreamReader(new BufferedInputStream(socket.getInputStream, 1024 * 1024), allocator)
      var count = 0
      var sum = 0L
      while (reader.loadNextBatch()) {
        val vector = reader.getVectorSchemaRoot.getVector(0).asInstanceOf[BigIntVector]
        var i = 0
        while (i < reader.getVectorSchemaRoot.getRowCount) {
          sum += vector.get(i)
          count += 1
          i += 1
        }
      }
      assert(count == 10000)
      assert(sum == 49995000L)
      assert(waitClosed(result(0).asInstanceOf[java.net.ServerSocket], 5000))
    } finally {
      if (reader != null) reader.close()
      allocator.close()
      if (socket != null) socket.close()
      ctx.close
    }
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == before)
  }

  test("empty partition still produces a readable Arrow stream") {
    val ctx = new JavaContext
    val app = new AppContextImpl(ctx, null)
    val runner = new SparkSocketRunner("serve-empty", "127.0.0.1", "UTC")
    val valueSchema = StructType(Seq(StructField("value", LongType, nullable = false)))
    var socket: Socket = null
    var reader: ArrowStreamReader = null
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("serve-empty", 0, Long.MaxValue)
    try {
      val result = runner.serveToStreamWithArrow(Iterator.empty, valueSchema, 1000, app)
      socket = new Socket()
      socket.connect(new InetSocketAddress("127.0.0.1", portOf(result)), 5000)
      reader = new ArrowStreamReader(new BufferedInputStream(socket.getInputStream, 65536), allocator)
      assert(!reader.loadNextBatch())
      assert(waitClosed(result(0).asInstanceOf[java.net.ServerSocket], 5000))
    } finally {
      if (reader != null) reader.close()
      allocator.close()
      if (socket != null) socket.close()
      ctx.close
    }
  }

  test("accept timeout fails the serve instead of looking like success") {
    val runner = new SparkSocketRunner("serve-timeout", "127.0.0.1", "UTC")
    val result = runner.serveToStream("serve-timeout", Map(
      "python.socket.accept.timeout" -> "400")) { _ => () }
    val server = result(0).asInstanceOf[java.net.ServerSocket]
    var failed: SparkException = null
    val deadline = System.currentTimeMillis + 5000
    while (failed == null && System.currentTimeMillis < deadline) {
      try {
        if (server.isClosed) throw new AssertionError("serve closed without reporting the timeout")
      } catch {
        case e: SparkException => failed = e
      }
      if (failed == null) Thread.sleep(20)
    }
    assert(failed != null)
    assert(failed.getCause.isInstanceOf[java.net.SocketTimeoutException])
  }

  test("task completion closes the listening socket without a serve failure") {
    val ctx = new JavaContext
    val app = new AppContextImpl(ctx, null)
    val runner = new SparkSocketRunner("serve-cancel", "127.0.0.1", "UTC")
    val result = runner.serveToStreamWithArrow(
      Iterator.empty, schema, 10, app, Map("python.socket.accept.timeout" -> "60000"))
    val server = result(0).asInstanceOf[java.net.ServerSocket]
    ctx.close
    assert(waitClosed(server, 5000))
  }

  test("reader reset fails the serve") {
    val runner = new SparkSocketRunner("serve-reset", "127.0.0.1", "UTC")
    val result = runner.serveToStream("serve-reset") { out =>
      val chunk = new Array[Byte](1024 * 1024)
      var n = 0
      while (n < 64) {
        out.write(chunk)
        out.flush()
        n += 1
      }
    }
    val socket = new Socket()
    socket.connect(new InetSocketAddress("127.0.0.1", portOf(result)), 5000)
    socket.close()
    val server = result(0).asInstanceOf[java.net.ServerSocket]
    var failed: SparkException = null
    val deadline = System.currentTimeMillis + 5000
    while (failed == null && System.currentTimeMillis < deadline) {
      try {
        if (server.isClosed) throw new AssertionError("reset was treated as a successful serve")
      } catch {
        case e: SparkException => failed = e
      }
      if (failed == null) Thread.sleep(20)
    }
    assert(failed != null)
    assert(failed.getCause.isInstanceOf[java.io.IOException])
  }

  test("transfer sockets disable Nagle") {
    val socket = new Socket()
    try {
      ArrowSockets.prepare(socket, 1024 * 1024)
      assert(socket.getTcpNoDelay)
      assert(socket.getKeepAlive)
    } finally socket.close()
  }
}
