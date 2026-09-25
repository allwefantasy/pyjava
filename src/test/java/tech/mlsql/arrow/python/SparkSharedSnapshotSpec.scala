package tech.mlsql.arrow.python

import java.io.{BufferedReader, DataInputStream, DataOutputStream, InputStreamReader}
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.{SparkConf, SparkContext, SparkException, TaskContext, WowRowEncoder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.{ArrowSnapshotServices, SnapshotProtocol, SparkSocketRunner}

class SparkSharedSnapshotSpec extends AnyFunSuite {
  private val python = sys.env("PYJAVA_TEST_PYTHON")
  private val valueSchema = StructType(Seq(StructField("value", LongType, nullable = false)))

  private def rowsOf(values: Seq[Long]): Iterator[InternalRow] = {
    val encoder = WowRowEncoder.fromRow(valueSchema)
    values.iterator.map(v => encoder(Row(v)).copy())
  }

  private def export(runner: SparkSocketRunner, values: Seq[Long],
                     conf: Map[String, String], ctx: JavaContext = null) = {
    val context = new AppContextImpl(Option(ctx).getOrElse(new JavaContext), null)
    val descriptor = runner.exportToStreamWithArrow(
      rowsOf(values), valueSchema, 1000, context, conf)
    (descriptor, context.innerContext.asInstanceOf[JavaContext])
  }

  private def probe(port: Int, times: Int): Unit = {
    for (_ <- 0 until times) {
      val socket = new Socket()
      socket.connect(new InetSocketAddress("127.0.0.1", port), 2000)
      socket.close()
    }
  }

  test("export commits a snapshot that survives producer completion") {
    val runner = new SparkSocketRunner("shared-export", "127.0.0.1", "UTC")
    val ctx = new JavaContext
    val (descriptor, producerCtx) = export(runner, (0L until 5000L), Map(
      "python.socket.shared.name" -> "spec-basic"), ctx)
    assert(descriptor.protocol == SnapshotProtocol.ProtocolName)
    assert(descriptor.token.nonEmpty)
    assert(descriptor.snapshotBytes > 0)
    producerCtx.close
    val readerCtx = new JavaContext
    try {
      val sum = runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
        descriptor.token, new AppContextImpl(readerCtx, null)).map(_.getLong(0)).sum
      assert(sum == 4999L * 5000L / 2)
      assert(runner.releaseSharedSnapshot(descriptor.host, descriptor.port,
        descriptor.token))
      val (status, _) = runner.sharedSnapshotStatus(descriptor.host, descriptor.port,
        descriptor.token, Map.empty)
      assert(status == SnapshotProtocol.StExpired)
    } finally readerCtx.close
  }

  test("bare connect/close probes never consume the snapshot") {
    val runner = new SparkSocketRunner("shared-probe", "127.0.0.1", "UTC")
    val (descriptor, _) = export(runner, (0L until 100L), Map(
      "python.socket.shared.name" -> "spec-probe"))
    probe(descriptor.port, 5)
    val ctx = new JavaContext
    try {
      val rows = runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
        descriptor.token, new AppContextImpl(ctx, null))
      assert(rows.map(_.getLong(0)).sum == 4950L)
    } finally ctx.close
  }

  test("unknown tokens and garbage frames are bounded") {
    val runner = new SparkSocketRunner("shared-bad", "127.0.0.1", "UTC")
    val (descriptor, _) = export(runner, Seq(1L), Map(
      "python.socket.shared.name" -> "spec-bad"))
    val (status, _) = runner.sharedSnapshotStatus(descriptor.host, descriptor.port,
      "no-such-token", Map.empty)
    assert(status == SnapshotProtocol.StUnknown)
    val socket = new Socket()
    socket.connect(new InetSocketAddress("127.0.0.1", descriptor.port), 2000)
    socket.setSoTimeout(5000)
    val out = new DataOutputStream(socket.getOutputStream)
    out.writeInt(0x0bad0bad)
    out.writeInt(1)
    out.writeInt(SnapshotProtocol.OpStatus)
    out.writeInt(4)
    out.write("junk".getBytes(StandardCharsets.UTF_8))
    out.flush()
    intercept[java.io.IOException] {
      new DataInputStream(socket.getInputStream).readInt()
    }
    socket.close()
    val ctx = new JavaContext
    try {
      assert(runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
        descriptor.token, new AppContextImpl(ctx, null)).map(_.getLong(0)).sum == 1L)
    } finally ctx.close
  }

  test("expired lease fails status and read without dropping a live snapshot") {
    val runner = new SparkSocketRunner("shared-lease", "127.0.0.1", "UTC")
    val conf = Map("python.socket.shared.name" -> "spec-lease",
      "python.socket.shared.lease.ms" -> "400")
    val (descriptor, _) = export(runner, (0L until 10L), conf)
    Thread.sleep(1200)
    val (status, _) = runner.sharedSnapshotStatus(descriptor.host, descriptor.port,
      descriptor.token, Map.empty)
    assert(status == SnapshotProtocol.StExpired)
    val ctx = new JavaContext
    try {
      intercept[SparkException] {
        runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
          descriptor.token, new AppContextImpl(ctx, null)).hasNext
      }
    } finally ctx.close
  }

  test("failed export is queryable and a later attempt gets a fresh token") {
    val runner = new SparkSocketRunner("shared-fail", "127.0.0.1", "UTC")
    val conf = Map("python.socket.shared.name" -> "spec-fail",
      "python.socket.shared.lease.ms" -> "30000")
    val ctx = new JavaContext
    val broken = rowsOf(0L until 10000L).zipWithIndex.map { case (row, i) =>
      if (i == 500) throw new RuntimeException("iterator exploded")
      row
    }
    intercept[RuntimeException] {
      runner.exportToStreamWithArrow(broken, valueSchema, 1000,
        new AppContextImpl(ctx, null), conf)
    }
    val service = ArrowSnapshotServices.get("127.0.0.1", conf)
    val failedToken = service.tokens.head
    val (status, info) = runner.sharedSnapshotStatus("127.0.0.1", service.port,
      failedToken, Map.empty)
    assert(status == SnapshotProtocol.StFailed)
    assert(info.contains("iterator exploded"))
    val readerCtx = new JavaContext
    try {
      intercept[SparkException] {
        runner.readFromSharedSnapshot("127.0.0.1", service.port, failedToken,
          new AppContextImpl(readerCtx, null)).hasNext
      }
    } finally readerCtx.close
    val (descriptor, _) = export(runner, Seq(7L), conf)
    assert(descriptor.token != failedToken)
    val ctx2 = new JavaContext
    try {
      assert(runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
        descriptor.token, new AppContextImpl(ctx2, null)).map(_.getLong(0)).sum == 7L)
    } finally ctx2.close
    ctx.close
  }

  test("process byte budget rejects a new export but keeps committed snapshots") {
    val runner = new SparkSocketRunner("shared-quota", "127.0.0.1", "UTC")
    val conf = Map("python.socket.shared.name" -> "spec-quota",
      "python.socket.shared.total.maxBytes" -> "65536")
    val (first, _) = export(runner, (0L until 100L), conf)
    val service = ArrowSnapshotServices.get("127.0.0.1", conf)
    assert(service.usedBytes == first.snapshotBytes)
    intercept[Exception] {
      val ctx = new JavaContext
      runner.exportToStreamWithArrow(rowsOf(0L until 1000000L), valueSchema, 1000,
        new AppContextImpl(ctx, null), conf)
    }
    val ctx = new JavaContext
    try {
      assert(runner.readFromSharedSnapshot(first.host, first.port, first.token,
        new AppContextImpl(ctx, null)).map(_.getLong(0)).sum == 4950L)
    } finally ctx.close
    assert(runner.releaseSharedSnapshot(first.host, first.port, first.token))
    val deadline = System.currentTimeMillis() + 5000
    while (service.usedBytes != 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20)
    }
    assert(service.usedBytes == 0)
  }

  test("one connection carries requests for different tokens") {
    val runner = new SparkSocketRunner("shared-multi", "127.0.0.1", "UTC")
    val conf = Map("python.socket.shared.name" -> "spec-multi")
    val (a, _) = export(runner, (0L until 100L), conf)
    val (b, _) = export(runner, (100L until 200L), conf)
    assert(a.port == b.port && a.token != b.token)
    val socket = new Socket()
    socket.connect(new InetSocketAddress("127.0.0.1", a.port), 5000)
    socket.setSoTimeout(10000)
    try {
      val in = new DataInputStream(socket.getInputStream)
      val out = new DataOutputStream(socket.getOutputStream)
      def readAll(token: String, expected: Long): Unit = {
        SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpRead, token)
        out.flush()
        val (status, _) = SnapshotProtocol.readResponse(in)
        assert(status == SnapshotProtocol.StReady)
        val length = in.readLong()
        val bytes = new Array[Byte](length.toInt)
        in.readFully(bytes)
        assert(bytes.length == expected)
      }
      readAll(a.token, a.snapshotBytes)
      readAll(b.token, b.snapshotBytes)
      SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpRelease, a.token)
      out.flush()
      assert(SnapshotProtocol.readResponse(in)._1 == SnapshotProtocol.StReady)
      SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpStatus, a.token)
      out.flush()
      assert(SnapshotProtocol.readResponse(in)._1 == SnapshotProtocol.StExpired)
      SnapshotProtocol.writeRequest(out, SnapshotProtocol.OpStatus, b.token)
      out.flush()
      assert(SnapshotProtocol.readResponse(in)._1 == SnapshotProtocol.StReady)
    } finally socket.close()
  }

  test("local[1]: partition exports end their tasks and stay readable later") {
    val sc = new SparkContext(new SparkConf().setMaster("local[1]")
      .setAppName("shared-lifecycle").set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1"))
    try {
      val conf = Map("python.socket.shared.name" -> "spec-local1")
      val descriptors = sc.parallelize(1 to 32, 1).mapPartitions { input =>
        // Keep the closure free of spec-instance captures so it stays serializable.
        val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
        val runner = new SparkSocketRunner("shared-local1", "127.0.0.1", "UTC")
        Iterator(runner.exportToStreamWithArrow(
          input.map(i => new GenericInternalRow(Array[Any](i.toLong))),
          schema, 10, new SparkContextImp(TaskContext.get(), null), conf))
      }.collect()
      assert(descriptors.length == 1)
      // The producer slot is free: a second job schedules without consuming first.
      assert(sc.parallelize(1 to 4).map(_ * 2).collect().toSeq == Seq(2, 4, 6, 8))
      val descriptor = descriptors.head
      assert(descriptor.partitionId == 0)
      val runner = new SparkSocketRunner("shared-local1-read", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val sum = runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
          descriptor.token, new AppContextImpl(ctx, null)).map(_.getLong(0)).sum
        assert(sum == 32L * 33L / 2)
      } finally ctx.close
    } finally sc.stop()
  }

  test("local[2]: partitions share the executor listener and stay isolated") {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]")
      .setAppName("shared-local2").set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1"))
    try {
      val conf = Map("python.socket.shared.name" -> "spec-local2")
      val descriptors = sc.parallelize(1 to 4000, 2).mapPartitions { input =>
        // Keep the closure free of spec-instance captures so it stays serializable.
        val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
        val runner = new SparkSocketRunner("shared-local2", "127.0.0.1", "UTC")
        Iterator(runner.exportToStreamWithArrow(
          input.map(i => new GenericInternalRow(Array[Any](i.toLong))),
          schema, 1000, new SparkContextImp(TaskContext.get(), null), conf))
      }.collect().sortBy(_.partitionId)
      assert(descriptors.length == 2)
      assert(descriptors(0).port == descriptors(1).port)
      assert(descriptors(0).token != descriptors(1).token)
      assert(descriptors(0).partitionId != descriptors(1).partitionId)
      val runner = new SparkSocketRunner("shared-local2-read", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val sums = descriptors.map { d =>
          runner.readFromSharedSnapshot(d.host, d.port, d.token,
            new AppContextImpl(ctx, null)).map(_.getLong(0)).sum
        }
        assert(sums.sum == 4000L * 4001L / 2)
        assert(sums(0) != sums(1))
      } finally ctx.close
    } finally sc.stop()
  }

  test("python client reads a JVM shared snapshot") {
    val runner = new SparkSocketRunner("shared-pyread", "127.0.0.1", "UTC")
    val (descriptor, _) = export(runner, (0L until 10000L), Map(
      "python.socket.shared.name" -> "spec-pyread"))
    val builder = new ProcessBuilder(python, "src/test/resources/shared_snapshot_client.py",
      descriptor.host, descriptor.port.toString, descriptor.token)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.environment().put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    val process = builder.start()
    try {
      val output = new BufferedReader(new InputStreamReader(process.getInputStream,
        StandardCharsets.UTF_8)).readLine()
      assert(process.waitFor(30, TimeUnit.SECONDS))
      assert(process.exitValue() == 0)
      assert(output == "10000 49995000")
    } finally process.destroyForcibly()
  }

  test("JVM reads a python shared snapshot and releases it") {
    val count = 1000
    val builder = new ProcessBuilder(python, "src/test/resources/shared_snapshot_server.py",
      "serve", count.toString).redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.environment().put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    val process = builder.start()
    val lines = new BufferedReader(new InputStreamReader(process.getInputStream,
      StandardCharsets.UTF_8))
    try {
      val port = lines.readLine().toInt
      val token = lines.readLine()
      val runner = new SparkSocketRunner("shared-pyserver", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val sum = runner.readFromSharedSnapshot("127.0.0.1", port, token,
          new AppContextImpl(ctx, null)).map(_.getLong(0)).sum
        assert(sum == (count - 1L) * count / 2)
      } finally ctx.close
      assert(runner.releaseSharedSnapshot("127.0.0.1", port, token))
      val (status, _) = runner.sharedSnapshotStatus("127.0.0.1", port, token, Map.empty)
      assert(status == SnapshotProtocol.StExpired)
    } finally {
      process.getOutputStream.close()
      process.waitFor(10, TimeUnit.SECONDS)
      process.destroyForcibly()
    }
  }

  test("JVM surfaces a failed python generation instead of a network timeout") {
    val builder = new ProcessBuilder(python, "src/test/resources/shared_snapshot_server.py",
      "fail").redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.environment().put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    val process = builder.start()
    val lines = new BufferedReader(new InputStreamReader(process.getInputStream,
      StandardCharsets.UTF_8))
    try {
      val port = lines.readLine().toInt
      val token = lines.readLine()
      val runner = new SparkSocketRunner("shared-pyfail", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val failure = intercept[SparkException] {
          runner.readFromSharedSnapshot("127.0.0.1", port, token,
            new AppContextImpl(ctx, null), Map(
              "python.socket.shared.wait.ready.ms" -> "5000")).hasNext
        }
        assert(failure.getMessage.contains("failed"))
        assert(failure.getMessage.contains("intentional generation failure"))
      } finally ctx.close
    } finally {
      process.getOutputStream.close()
      process.waitFor(10, TimeUnit.SECONDS)
      process.destroyForcibly()
    }
  }

  test("JVM waits for a slow python producer then reads") {
    val builder = new ProcessBuilder(python, "src/test/resources/shared_snapshot_server.py",
      "slow", "500").redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.environment().put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    val process = builder.start()
    val lines = new BufferedReader(new InputStreamReader(process.getInputStream,
      StandardCharsets.UTF_8))
    try {
      val port = lines.readLine().toInt
      val token = lines.readLine()
      val runner = new SparkSocketRunner("shared-pyslow", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val sum = runner.readFromSharedSnapshot("127.0.0.1", port, token,
          new AppContextImpl(ctx, null), Map(
            "python.socket.shared.wait.ready.ms" -> "30000")).map(_.getLong(0)).sum
        assert(sum == 499L * 500L / 2)
      } finally ctx.close
    } finally {
      process.getOutputStream.close()
      process.waitFor(10, TimeUnit.SECONDS)
      process.destroyForcibly()
    }
  }

  test("a later process budget does not silently replace the first") {
    val name = Map("python.socket.shared.name" -> "spec-conflict")
    val first = ArrowSnapshotServices.get("127.0.0.1", name)
    val conflict = intercept[SparkException] {
      ArrowSnapshotServices.get("127.0.0.1", name + (
        "python.socket.shared.total.maxBytes" -> "1024"))
    }
    assert(conflict.getMessage.contains("total.maxBytes"))
    assert(first.totalMaxBytes == 4294967296L)
    assert(ArrowSnapshotServices.get("127.0.0.1", name) eq first)
  }

  test("release keeps the byte budget until the last reader closes") {
    val runner = new SparkSocketRunner("shared-readers", "127.0.0.1", "UTC")
    val conf = Map("python.socket.shared.name" -> "spec-readers")
    val (descriptor, _) = export(runner, 0L until 1000L, conf)
    val service = ArrowSnapshotServices.get("127.0.0.1", conf)
    val entry = service.entry(descriptor.token).get
    val input = entry.openReader()
    try {
      assert(entry.openReaders == 1)
      assert(runner.releaseSharedSnapshot(descriptor.host, descriptor.port, descriptor.token, conf))
      assert(service.usedBytes == descriptor.snapshotBytes)
      assert(java.nio.file.Files.exists(entry.snapshotPath))
      val (status, _) = runner.sharedSnapshotStatus(descriptor.host, descriptor.port,
        descriptor.token, Map.empty)
      assert(status == SnapshotProtocol.StExpired)
    } finally {
      input.close()
      entry.closeReader()
      service.settleQuiet(entry)
    }
    assert(service.usedBytes == 0)
    assert(!java.nio.file.Files.exists(entry.snapshotPath))
  }

  test("release while preparing stops the writer and returns the budget") {
    val conf = Map("python.socket.shared.name" -> "spec-cancel-write",
      "python.socket.shared.total.maxBytes" -> "10485760")
    val runner = new SparkSocketRunner("shared-cancel-write", "127.0.0.1", "UTC")
    ArrowSnapshotServices.get("127.0.0.1", conf)
    val service = ArrowSnapshotServices.get("127.0.0.1", conf)
    val started = new CountDownLatch(1)
    val encoder = WowRowEncoder.fromRow(valueSchema)
    val iter = Iterator.continually {
      started.countDown()
      Thread.sleep(20)
      encoder(Row(1L)).copy()
    }
    val errors = new AtomicReference[Throwable]()
    val thread = new Thread(new Runnable {
      override def run(): Unit = try {
        runner.exportToStreamWithArrow(iter, valueSchema, 1,
          new AppContextImpl(new JavaContext, null), conf)
        ()
      } catch {
        case t: Throwable => errors.set(t)
      }
    })
    thread.start()
    assert(started.await(5, TimeUnit.SECONDS))
    Thread.sleep(50)
    val token = service.tokens.head
    assert(runner.releaseSharedSnapshot("127.0.0.1", service.port, token, conf))
    thread.join(5000)
    assert(!thread.isAlive)
    assert(errors.get != null)
    val deadline = System.currentTimeMillis() + 3000
    while (service.usedBytes != 0 && System.currentTimeMillis() < deadline) Thread.sleep(20)
    assert(service.usedBytes == 0)
  }

  test("cancellation during status wait closes the socket") {
    val conf = Map("python.socket.shared.name" -> "spec-cancel-wait",
      "python.socket.shared.prepare.timeout.ms" -> "60000")
    val service = ArrowSnapshotServices.get("127.0.0.1", conf)
    val entry = service.register(0L, 0L, conf)
    val runner = new SparkSocketRunner("shared-cancel-wait", "127.0.0.1", "UTC")
    val ctx = new JavaContext
    val errors = new AtomicReference[Throwable]()
    val thread = new Thread(new Runnable {
      override def run(): Unit = try {
        runner.readFromSharedSnapshot("127.0.0.1", service.port, entry.token,
          new AppContextImpl(ctx, null), Map(
            "python.socket.shared.wait.ready.ms" -> "30000",
            "python.socket.shared.status.poll.ms" -> "50")).hasNext
        ()
      } catch {
        case t: Throwable => errors.set(t)
      }
    })
    thread.start()
    Thread.sleep(200)
    val began = System.nanoTime()
    ctx.markInterrupted("stop waiting")
    thread.join(5000)
    assert(!thread.isAlive)
    val error = errors.get
    assert(error != null)
    val reason = error match {
      case killed: tech.mlsql.arrow.python.iapp.TaskKilledException => killed.reason
      case other => String.valueOf(other.getMessage)
    }
    assert(reason.contains("stop waiting"))
    assert(System.nanoTime() - began < 3000000000L)
  }

  test("local[2] collects more partitions than slots, then rereads without releasing") {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]")
      .setAppName("shared-oversub").set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1"))
    try {
      val conf = Map("python.socket.shared.name" -> "spec-oversub")
      val descriptors = sc.parallelize(1 to 800, 4).mapPartitions { input =>
        val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
        val runner = new SparkSocketRunner("shared-oversub", "127.0.0.1", "UTC")
        Iterator(runner.exportToStreamWithArrow(
          input.map(i => new GenericInternalRow(Array[Any](i.toLong))),
          schema, 200, new SparkContextImp(TaskContext.get(), null), conf))
      }.collect()
      assert(descriptors.length == 4)
      assert(descriptors.map(_.partitionId).distinct.length == 4)
      assert(sc.parallelize(1 to 2, 2).count() == 2)
      val runner = new SparkSocketRunner("shared-oversub-read", "127.0.0.1", "UTC")
      val ctx = new JavaContext
      try {
        val sums = descriptors.map { descriptor =>
          val once = runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
            descriptor.token, new AppContextImpl(ctx, null)).map(_.getLong(0)).toList
          val again = runner.readFromSharedSnapshot(descriptor.host, descriptor.port,
            descriptor.token, new AppContextImpl(ctx, null)).map(_.getLong(0)).toList
          assert(once == again)
          val (status, _) = runner.sharedSnapshotStatus(descriptor.host, descriptor.port,
            descriptor.token, Map.empty)
          assert(status == SnapshotProtocol.StReady)
          once.sum
        }
        assert(sums.sum == 800L * 801 / 2)
      } finally ctx.close
    } finally sc.stop()
  }

  test("a failed attempt after export is cleaned up and the retry uses a new token") {
    // local[N] hard-codes one task failure. local[N,F] is what actually retries.
    val sc = new SparkContext(new SparkConf().setMaster("local[1,2]")
      .setAppName("shared-retry").set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1"))
    try {
      val conf = Map("python.socket.shared.name" -> "spec-retry")
      val descriptors = sc.parallelize(Seq(1), 1).mapPartitions { _ =>
        val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
        val task = TaskContext.get()
        val runner = new SparkSocketRunner("shared-retry", "127.0.0.1", "UTC")
        val descriptor = runner.exportToStreamWithArrow(
          Iterator(new GenericInternalRow(Array[Any](Long.box(task.attemptNumber().toLong + 1L)))),
          schema, 10, new SparkContextImp(task, null), conf)
        if (task.attemptNumber() == 0) {
          throw new RuntimeException("fail after export")
        }
        Iterator(descriptor)
      }.collect()
      assert(descriptors.length == 1)
      assert(descriptors.head.partitionId == 0)
      // Failed tasks do not publish accumulator updates. The failed attempt
      // stays on the shared listener under a different token.
      val service = ArrowSnapshotServices.get("127.0.0.1", conf)
      val failedTokens = service.tokens - descriptors.head.token
      assert(failedTokens.size == 1)
      val failedToken = failedTokens.head
      val runner = new SparkSocketRunner("shared-retry-read", "127.0.0.1", "UTC")
      val (status, info) = runner.sharedSnapshotStatus(descriptors.head.host,
        descriptors.head.port, failedToken, Map.empty)
      assert(status == SnapshotProtocol.StFailed || status == SnapshotProtocol.StExpired)
      assert(info.contains("fail after export") || status == SnapshotProtocol.StExpired)
      val ctx = new JavaContext
      try {
        val values = runner.readFromSharedSnapshot(descriptors.head.host, descriptors.head.port,
          descriptors.head.token, new AppContextImpl(ctx, null)).map(_.getLong(0)).toList
        assert(values == List(2L))
      } finally ctx.close
    } finally sc.stop()
  }
}
