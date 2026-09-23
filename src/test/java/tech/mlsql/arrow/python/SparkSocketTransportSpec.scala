package tech.mlsql.arrow.python

import java.io.DataInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.ArrowUtils
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.SparkSocketRunner

@DoNotDiscover
class SparkSocketTransportSpec extends AnyFunSuite {
  private val python = sys.env("PYJAVA_TEST_PYTHON")

  private def withServer(invalid: Boolean = false)(test: (Int, JavaContext) => Unit): Unit = {
    val builder = new ProcessBuilder(python, "src/test/resources/arrow_socket_server.py")
      .redirectError(ProcessBuilder.Redirect.INHERIT)
    if (invalid) builder.environment().put("INVALID_END", "1")
    val process = builder.start()
    val ctx = new JavaContext
    try {
      val port = new DataInputStream(process.getInputStream).readInt()
      test(port, ctx)
      ctx.close
      assert(process.waitFor(10, TimeUnit.SECONDS), "one-shot server did not observe client close")
      assert(process.exitValue() == 0)
    } finally {
      ctx.close
      process.destroyForcibly()
      process.getInputStream.close()
      process.getOutputStream.close()
    }
  }

  test("one-shot Arrow stream closes socket after acknowledgement") {
    withServer() { (port, ctx) =>
      val reader = new SparkSocketRunner("test", "127.0.0.1", "UTC")
      val rows = reader.readFromStreamWithArrow("127.0.0.1", port,
        new AppContextImpl(ctx, null), Map("python.socket.read.timeout" -> "5000"))
      assert(rows.map(_.getLong(0)).sum == 99999L * 100000L / 2)
    }
  }

  test("one-shot early close releases reader memory and socket") {
    val before = ArrowUtils.rootAllocator.getAllocatedMemory
    withServer() { (port, ctx) =>
      val reader = new SparkSocketRunner("test", "127.0.0.1", "UTC")
      val rows = reader.readFromStreamWithArrow("127.0.0.1", port, new AppContextImpl(ctx, null))
      assert(rows.next().getLong(0) == 0)
      ctx.close
      assert(ArrowUtils.rootAllocator.getAllocatedMemory == before)
    }
  }

  test("python client reads a served Spark partition") {
    val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
    val encoder = WowRowEncoder.fromRow(schema)
    val rows = (0 until 10000).iterator.map(i => encoder(Row(i.toLong)).copy())
    val ctx = new JavaContext
    val runner = new SparkSocketRunner("serve", "127.0.0.1", "UTC")
    var process: Process = null
    try {
      val result = runner.serveToStreamWithArrow(rows, schema, 1000, new AppContextImpl(ctx, null))
      val port = result(2).asInstanceOf[Number].intValue
      process = new ProcessBuilder(python, "src/test/resources/arrow_socket_client.py", port.toString)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start()
      val output = scala.io.Source.fromInputStream(process.getInputStream, StandardCharsets.UTF_8.name()).mkString.trim
      assert(process.waitFor(20, TimeUnit.SECONDS), "python client did not finish reading the partition")
      assert(process.exitValue() == 0)
      assert(output == "10000 49995000")
    } finally {
      ctx.close
      if (process != null) process.destroyForcibly()
    }
  }

  test("Spark reads a partition served by Ray OnceServer") {
    val count = 1000
    val process = new ProcessBuilder(python, "src/test/resources/once_server_rows.py", count.toString)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
    process.environment().put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    val started = process.start()
    val ctx = new JavaContext
    try {
      val port = new DataInputStream(started.getInputStream).readInt()
      val reader = new SparkSocketRunner("ray-return", "127.0.0.1", "UTC")
      val rows = reader.readFromStreamWithArrow("127.0.0.1", port,
        new AppContextImpl(ctx, null), Map("python.socket.read.timeout" -> "15000"))
      assert(rows.map(_.getLong(0)).sum == (count - 1L) * count / 2)
      assert(started.waitFor(15, TimeUnit.SECONDS), "OnceServer did not finish after the Spark ack")
      assert(started.exitValue() == 0)
    } finally {
      ctx.close
      started.destroyForcibly()
      started.getInputStream.close()
      started.getOutputStream.close()
    }
  }

  test("invalid one-shot end marker fails instead of silently accepting the stream") {
    withServer(invalid = true) { (port, ctx) =>
      val reader = new SparkSocketRunner("test", "127.0.0.1", "UTC")
      intercept[Exception] {
        reader.readFromStreamWithArrow("127.0.0.1", port, new AppContextImpl(ctx, null)).foreach(_ => ())
      }
    }
  }
}
