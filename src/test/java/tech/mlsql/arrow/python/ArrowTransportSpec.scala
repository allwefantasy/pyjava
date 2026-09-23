package tech.mlsql.arrow.python

import java.nio.file.Paths
import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.ArrowUtils
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonFunction}
import scala.collection.JavaConverters._

/** Explicit opt-in: PYJAVA_TEST_PYTHON must have pandas, pyarrow and requests. */
@DoNotDiscover
class ArrowTransportSpec extends AnyFunSuite with BeforeAndAfterEach {
  private val python = sys.env.getOrElse("PYJAVA_TEST_PYTHON",
    throw new IllegalArgumentException("Set PYJAVA_TEST_PYTHON to the integration-test Python"))
  private val schema = StructType(Seq(StructField("value", LongType)))
  private val defaults = Map("python.worker.startup.timeout" -> "15000",
    "python.socket.read.timeout" -> "15000", "PY_EXECUTE_USER" -> "", "groupId" -> "")

  override def afterEach(): Unit = PythonWorkerFactory.shutdownAll()

  private def runner(command: String, options: Map[String, String] = Map.empty): ArrowPythonRunner = {
    val env = new util.HashMap[String, String]()
    env.put("PYTHONPATH", Paths.get("python").toAbsolutePath.toString)
    new ArrowPythonRunner(Seq(ChainedPythonFunctions(Seq(
      PythonFunction(command, env, python, "3")))), schema, "UTC", defaults ++ options)
  }

  private def run(command: String, rows: Int = 10, options: Map[String, String] = Map.empty): Vector[Vector[Long]] = {
    val r = runner(command, options)
    val ctx = new JavaContext
    val task = new AppContextImpl(ctx, r)
    val input = (0 until rows).iterator.map(i => new GenericInternalRow(Array[Any](i.toLong)))
    try r.compute(Iterator(input), 0, task).flatMap { batch =>
      batch.rowIterator().asScala.map(row => (0 until row.numFields).map(row.getLong).toVector)
    }.toVector
    finally ctx.close
  }

  test("real Java-Python Arrow requests reuse one worker process") {
    val pids = (1 to 20).map { _ =>
      run("import os\ncontext.build_result([{'pid': os.getpid()}])").head.head
    }
    assert(pids.distinct.size == 1)
  }

  test("large partitions are bounded by the configured Arrow record count") {
    val result = run(
      "sizes = [len(df) for df in context.fetch_once()]\n" +
        "context.build_result([{'rows': sum(sizes), 'largest': max(sizes), 'batches': len(sizes)}])",
      25001, Map("python.arrow.maxRecordsPerBatch" -> "1024"))
    assert(result == Vector(Vector(25001L, 1024L, 25L)))
  }

  test("ignored and partially consumed input allow a subsequent task on the same worker") {
    val first = run("import os\ncontext.build_result([{'pid': os.getpid()}])", 200000).head.head
    val second = run("import os\nnext(context.fetch_once_as_rows())\n" +
      "context.build_result([{'pid': os.getpid()}])", 200000).head.head
    assert(second == first)
  }

  test("Python errors, abrupt exits and writer failures discard their worker") {
    assert(intercept[Exception](run("raise ValueError('transport regression')"))
      .getMessage.contains("transport regression"))
    intercept[Exception](run("import os\nos._exit(7)"))
    assert(run("context.build_result([{'ok': 1}])") == Vector(Vector(1L)))
    val r = runner("context.noops_fetch()")
    val ctx = new JavaContext
    val input = Iterator.continually[org.apache.spark.sql.catalyst.InternalRow] {
      throw new IllegalStateException("input failed")
    }
    try intercept[IllegalStateException] {
      r.compute(Iterator(input), 0, new AppContextImpl(ctx, r)).hasNext
    } finally ctx.close
    assert(run("context.build_result([{'ok': 2}])") == Vector(Vector(2L)))
  }

  test("early result close releases Arrow memory and cannot reuse an unfinished worker") {
    val before = ArrowUtils.rootAllocator.getAllocatedMemory
    val r = runner("context.build_result({'value': i} for i in range(100000))")
    val ctx = new JavaContext
    val output = r.compute(Iterator(Iterator.empty), 0, new AppContextImpl(ctx, r))
    assert(output.hasNext)
    output.next()
    ctx.close
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == before)
    assert(run("context.build_result([{'ok': 1}])") == Vector(Vector(1L)))
  }

  test("read timeout aborts a stuck task and the following task succeeds") {
    intercept[Exception] {
      run("import time\ntime.sleep(5)", options = Map("python.socket.read.timeout" -> "100"))
    }
    assert(run("context.build_result([{'ok': 1}])") == Vector(Vector(1L)))
  }

  test("standalone worker mode and disabled reuse finish the protocol cleanly") {
    assert(run("context.build_result([{'ok': 1}])",
      options = Map("python.use.daemon" -> "false")) == Vector(Vector(1L)))
    val pids = (1 to 3).map { _ =>
      run("import os\ncontext.build_result([{'pid': os.getpid()}])",
        options = Map("py_worker_reuse" -> "false")).head.head
    }
    assert(pids.distinct.size == 3)
  }

  test("parallel Java callers exchange independent Arrow streams") {
    val executor = Executors.newFixedThreadPool(4)
    try {
      val jobs = (1 to 12).map { i =>
        executor.submit(new Callable[Vector[Vector[Long]]] {
          override def call(): Vector[Vector[Long]] =
            run("context.build_result([{'sum': sum(r['value'] for r in context.fetch_once_as_rows())}])", i * 100)
        })
      }
      jobs.zipWithIndex.foreach { case (job, index) =>
        val n = (index + 1L) * 100L
        assert(job.get(30, TimeUnit.SECONDS) == Vector(Vector(n * (n - 1) / 2)))
      }
    } finally executor.shutdownNow()
  }

  test("many Arrow batches preserve every row across a complete round trip") {
    val rows = run("context.build_result({'value': r['value'] + 1} for r in context.fetch_once_as_rows())",
      25001, Map("python.arrow.maxRecordsPerBatch" -> "1024"))
    assert(rows.map(_.head) == (1L to 25001L).toVector)
  }

  test("task cancellation unblocks a reader even when the task thread is not interrupted") {
    val r = runner("import time\ntime.sleep(30)",
      Map("python.socket.read.timeout" -> "0", "python.task.killTimeout" -> "50"))
    val ctx = new JavaContext
    val executor = Executors.newSingleThreadExecutor()
    val ready = new java.util.concurrent.CountDownLatch(1)
    try {
      val result = executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          val output = r.compute(Iterator(Iterator.empty), 0, new AppContextImpl(ctx, r))
          ready.countDown()
          val error = intercept[RuntimeException](output.hasNext)
          assert(error.isInstanceOf[org.apache.spark.TaskKilledException] ||
            error.isInstanceOf[tech.mlsql.arrow.python.iapp.TaskKilledException])
        }
      })
      assert(ready.await(10, TimeUnit.SECONDS))
      ctx.markInterrupted("cancel transport test")
      result.get(5, TimeUnit.SECONDS)
    } finally {
      ctx.close
      executor.shutdownNow()
    }
    assert(run("context.build_result([{'ok': 1}])") == Vector(Vector(1L)))
  }

  test("barrier callbacks reach the task context and the same worker is reusable") {
    val command = "context.barrier()\nBarrierTaskContext.get().barrier()\n" +
      "import os\ncontext.build_result([{'pid': os.getpid()}])"
    val r = runner(command)
    val ctx = new CountingBarrierContext
    try {
      val pid = oneTask(r, ctx)
      assert(ctx.calls.get == 2)
      ctx.close
      assert(run("import os\ncontext.build_result([{'pid': os.getpid()}])").head.head == pid)
    } finally ctx.close
  }

  test("a failed barrier fails that task and the worker is not reused") {
    val before = run("import os\ncontext.build_result([{'pid': os.getpid()}])").head.head
    val r = runner("context.barrier()\ncontext.build_result([{'ok': 1}])")
    val ctx = new RejectingBarrierContext
    try {
      val error = intercept[Exception](oneTask(r, ctx))
      assert(error.getMessage.contains("barrier timed out"))
    } finally ctx.close
    val after = run("import os\ncontext.build_result([{'pid': os.getpid()}])").head.head
    assert(after != before)
  }

  test("barrier is rejected when the task did not open a callback socket") {
    val error = intercept[Exception](run("context.barrier()"))
    assert(error.getMessage.contains("barrier stage"))
    assert(run("context.build_result([{'ok': 1}])") == Vector(Vector(1L)))
  }

  test("record benchmark samples for warm reuse versus newly forked workers") {
    assume(sys.env.get("PYJAVA_BENCHMARK").contains("1"), "Set PYJAVA_BENCHMARK=1 to benchmark")
    if (sys.env.get("PYJAVA_BENCHMARK").contains("1")) {
      val command = "context.noops_fetch()\ncontext.build_result([{'ok': 1}])"
      for (reuse <- Seq(true, false)) {
        val options = Map("py_worker_reuse" -> reuse.toString)
        (1 to 5).foreach(_ => run(command, 1000, options))
        val samples = (1 to 30).map { _ =>
          val start = System.nanoTime()
          assert(run(command, 1000, options) == Vector(Vector(1L)))
          (System.nanoTime() - start) / 1000000.0
        }.sorted
        info(f"transport benchmark reuse=$reuse n=30 median_ms=${samples(15)}%.3f p95_ms=${samples(28)}%.3f")
      }
    }
  }

  private def oneTask(r: ArrowPythonRunner, ctx: JavaContext): Long = {
    val task = new BarrierTask(ctx, r)
    val values = r.compute(Iterator(Seq.empty[InternalRow].iterator), 3, task).flatMap { batch =>
      batch.rowIterator().asScala.map(_.getLong(0))
    }.toVector
    assert(values.size == 1)
    values.head
  }
}

private class CountingBarrierContext extends JavaContext {
  val calls = new java.util.concurrent.atomic.AtomicInteger()
  def barrier(): Unit = calls.incrementAndGet()
}

private class RejectingBarrierContext extends JavaContext {
  def barrier(): Unit = throw new SparkException("barrier timed out")
}

private class BarrierTask(context: JavaContext, runner: ArrowPythonRunner)
  extends AppContextImpl(context, runner) {
  override def isBarrier: Boolean = true
  override def innerContext: Any = context
}
