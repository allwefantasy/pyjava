package tech.mlsql.arrow.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicReference
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.{ArrowConverters, ArrowUtils}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner

class PartitionLifecycleSpec extends AnyFunSuite {
  test("detached Spark partition survives producing task completion") {
    val sc = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("partition-lifecycle")
      .set("spark.ui.enabled", "false").set("spark.driver.host", "127.0.0.1"))
    try {
      val ports = sc.parallelize(1 to 32, 1).mapPartitions { input =>
        val runner = new SparkSocketRunner("snapshot", "127.0.0.1", "UTC")
        val schema = StructType(Seq(StructField("id", LongType)))
        val result = runner.serveToStreamWithArrow(input.map(i => new GenericInternalRow(Array[Any](i.toLong))),
          schema, 10, new SparkContextImp(TaskContext.get(), null),
          Map("python.socket.detached" -> "true", "python.socket.accept.timeout" -> "5000"))
        Iterator(result(2).asInstanceOf[Number].intValue)
      }.collect()
      val sock = new Socket("127.0.0.1", ports.head)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator("detached-test", 0, 1024*1024)
      val reader = new ArrowStreamReader(sock.getInputStream, allocator)
      try {
        var count = 0
        while (reader.loadNextBatch()) count += reader.getVectorSchemaRoot.getRowCount
        assert(count == 32)
      } finally { reader.close(); allocator.close(); sock.close() }
    } finally sc.stop()
  }

  test("cancelled one-shot reader unblocks without task completion") {
    val server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
    val ctx = new JavaContext
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(new Runnable {
      override def run(): Unit = try {
        new SparkSocketRunner("cancel", "127.0.0.1", "UTC")
          .readFromStreamWithArrow("127.0.0.1", server.getLocalPort, new AppContextImpl(ctx, null),
            Map("python.socket.read.timeout" -> "0")).hasNext
      } catch { case e: Throwable => failure.set(e) }
    })
    thread.setDaemon(true)
    thread.start()
    val conn = server.accept()
    try {
      ctx.markInterrupted("cancel reader")
      thread.join(3000)
      assert(!thread.isAlive)
      assert(failure.get != null)
    } finally { ctx.close; conn.close(); server.close() }
  }

  test("wide rows split on byte budget and oversized rows fail without leaking") {
    val before = ArrowUtils.rootAllocator.getAllocatedMemory
    val schema = StructType(Seq(StructField("body", StringType)))
    val ctx = new JavaContext
    val out = new ByteArrayOutputStream()
    val rows = (0 until 20).iterator.map(_ => new GenericInternalRow(Array[Any](UTF8String.fromString("x" * 3000))))
    ArrowConverters.writeLegacyArrowStream(rows, schema, 10000, "UTC", out,
      new AppContextImpl(ctx, null), 4096, 1024*1024)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("wide-test", 0, 1024*1024)
    val reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray), allocator)
    try {
      var batches = 0
      var count = 0
      while (reader.loadNextBatch()) {
        assert(reader.getVectorSchemaRoot.getRowCount == 1)
        batches += 1
        count += reader.getVectorSchemaRoot.getRowCount
      }
      assert(count == 20 && batches >= 5)
    } finally { reader.close(); allocator.close(); ctx.close }
    intercept[IllegalArgumentException] {
      ArrowConverters.writeLegacyArrowStream(Iterator(new GenericInternalRow(Array[Any](UTF8String.fromString("x"*8192)))),
        schema, 10000, "UTC", new ByteArrayOutputStream(), new AppContextImpl(new JavaContext, null), 4096, 1024*1024)
    }
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == before)
  }
}
