package tech.mlsql.arrow.python

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner

/** Real Spark tasks -> real RayDataServer actors -> Spark tasks, including retry.
 * Uses only streaming rows/batches. collect() below contains two endpoints/counters, not data.
 */
object SparkRayIntegration {
  def main(args: Array[String]): Unit = {
    require(args.length == 6 || args.length == 7, "python fixture.py outputDir rowsPerPartition payloadBytes rows|batches [passes]")
    val directory = Paths.get(args(2)).toAbsolutePath
    Files.createDirectories(directory)
    require(!Files.exists(directory.resolve("ray-endpoints.tsv")), "Use a fresh output directory")
    val rows = args(3).toInt
    val width = args(4).toInt
    val mode = args(5)
    val passes = if (args.length == 7) args(6).toInt else 2
    require(passes >= 2 && passes <= 100)
    require(rows > 0 && width > 0 && Set("rows", "batches").contains(mode))
    val sc = new SparkContext(new SparkConf().setMaster("local[2,2]").setAppName("pyjava-real-spark-ray")
      .set("spark.ui.enabled", "false").set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1"))
    sc.setLogLevel("WARN")
    var python: Process = null
    val start = System.nanoTime()
    try {
      val endpoints = sc.parallelize(Seq(0, 1), 2).map { partition =>
        val schema = StructType(Seq(StructField("id", LongType, false),
          StructField("nullable_id", LongType, true), StructField("body", StringType, false)))
        val body = UTF8String.fromString("x" * width)
        val input = (0 until rows).iterator.map { i =>
          val id = partition.toLong * rows + i
          new GenericInternalRow(Array[Any](id, if (id % 31 == 0) null else 9007199254740993L + id, body))
        }
        val server = new SparkSocketRunner("real-spark-export", "127.0.0.1", "UTC")
        val result = server.serveToStreamWithArrow(input, schema, 8192,
          new SparkContextImp(TaskContext.get(), null), Map("python.socket.detached" -> "true",
            "python.socket.accept.timeout" -> "180000", "python.socket.spool.maxBytes" -> "2147483648"))
        result(1).toString + "\t" + result(2).toString
      }.collect()
      Files.write(directory.resolve("spark-sources.tsv"), endpoints.mkString("\n").getBytes(StandardCharsets.UTF_8))
      val builder = new ProcessBuilder(args(0), args(1), directory.toString, mode)
        .redirectOutput(directory.resolve("ray.log").toFile).redirectErrorStream(true)
      python = builder.start()
      val ready = directory.resolve("ray-endpoints.tsv")
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
      while (!Files.exists(ready) && python.isAlive && System.nanoTime() < deadline) Thread.sleep(100)
      require(Files.exists(ready), "Ray actors did not become ready; see ray.log")
      val source = scala.io.Source.fromFile(ready.toFile)
      val addresses = try source.getLines().map { line =>
        val parts = line.split("\t"); (parts(0), parts(1).toInt)
      }.toVector finally source.close()
      var retryObserved = false
      val consumeStart = System.nanoTime()
      val samples = new scala.collection.mutable.ArrayBuffer[String]
      for (pass <- 0 until passes) {
        val results = sc.parallelize(addresses, 2).mapPartitionsWithIndex { (partition, input) =>
          val (host, port) = input.next()
          val ctx = TaskContext.get()
          val iter = new SparkSocketRunner("real-ray-return", host, "UTC")
            .readFromStreamWithArrow(host, port, new SparkContextImp(ctx, null))
          val expectedBody = UTF8String.fromString("x" * width)
          var count = 0L
          while (iter.hasNext) {
            val row = iter.next()
            val id = partition.toLong * rows + count
            require(row.getLong(0) == id, "Missing, duplicate or reordered row")
            if (id % 31 == 0) require(row.isNullAt(1), "Null changed")
            else require(row.getLong(1) == 9007199254740993L + id, "int64 precision lost")
            require(row.getUTF8String(2).equals(expectedBody), "Payload changed")
            count += 1
            if (pass == 0 && partition == 0 && ctx.attemptNumber() == 0 && count == math.min(100, rows))
              throw new IOException("intentional consumer failure to verify real Spark task retry")
          }
          Iterator((count, ctx.attemptNumber()))
        }.collect()
        require(results.map(_._1).sum == rows.toLong * 2)
        retryObserved ||= results.exists(_._2 > 0)
        val allocatorBytes = tech.mlsql.arrow.ArrowUtils.rootAllocator.getAllocatedMemory
        require(allocatorBytes == 0, "Arrow buffers retained after completed Spark action")
        val os = java.lang.management.ManagementFactory.getOperatingSystemMXBean
        val fds = os.asInstanceOf[com.sun.management.UnixOperatingSystemMXBean].getOpenFileDescriptorCount
        samples += s"$pass,${(System.nanoTime() - consumeStart) / 1e9},$allocatorBytes,$fds"
        Files.write(directory.resolve("per-pass.csv"),
          ("pass,elapsed_seconds,arrow_allocated_bytes,jvm_fds\n" + samples.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
      }
      require(retryObserved, "The injected failure did not cause a Spark retry")
      val consumeSeconds = (System.nanoTime() - consumeStart) / 1e9
      val bytes = rows.toLong * 2 * passes * (width + 16L)
      val report = s"""{"mode":"$mode","spark":"${sc.version}","rows_per_partition":$rows,"payload_bytes":$width,"partitions":2,"successful_read_passes":$passes,"task_retry_verified":true,"logical_return_bytes":$bytes,"consume_seconds":$consumeSeconds,"logical_mib_per_second":${bytes / 1048576.0 / consumeSeconds},"total_seconds":${(System.nanoTime() - start) / 1e9}}"""
      Files.write(directory.resolve("report.json"), report.getBytes(StandardCharsets.UTF_8))
      println(report)
    } finally {
      if (python != null) {
        python.getOutputStream.close()
        if (!python.waitFor(30, TimeUnit.SECONDS)) python.destroyForcibly()
      }
      sc.stop()
    }
  }
}
