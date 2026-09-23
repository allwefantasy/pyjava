package tech.mlsql.arrow.python

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.nio.file.Paths
import java.util.concurrent.{Callable, Executors, TimeUnit}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class PythonWorkerFactorySpec extends AnyFunSuite with BeforeAndAfterEach {
  private val python = sys.env.getOrElse("PYJAVA_TEST_PYTHON", "python3")
  private val env = Map("PYTHONPATH" -> Paths.get("src/test/resources").toAbsolutePath.toString)
  private val conf = Map("python.daemon.module" -> "fake_worker_daemon",
    "python.worker.startup.timeout" -> "1000")
  override def afterEach(): Unit = PythonWorkerFactory.shutdownAll()

  private def acquire(options: Map[String, String] = conf, variables: Map[String, String] = env) =
    PythonWorkerFactory.createPythonWorker(python, variables, options)

  test("factory reuses healthy sockets and never leases a duplicate return twice") {
    val first = acquire()
    assert(first.getTcpNoDelay && first.getKeepAlive)
    assert(first.getSoTimeout == 0)
    PythonWorkerFactory.releasePythonWorker(python, env, first)
    PythonWorkerFactory.releasePythonWorker(python, env, first)
    assert(acquire() eq first)
    val second = acquire()
    assert(second ne first)
  }

  test("pool settings are isolated, while task-only options share a factory") {
    val first = acquire()
    PythonWorkerFactory.releasePythonWorker(python, env, first)
    assert(acquire(conf + ("timezone" -> "UTC")) eq first)
    PythonWorkerFactory.releasePythonWorker(python, env, first)
    val second = acquire(conf + ("python.socket.read.timeout" -> "1234"))
    assert(second ne first)
    assert(second.getSoTimeout == 1234)
  }

  test("a closed socket is replaced and the new worker exchanges data") {
    val first = acquire()
    first.close()
    PythonWorkerFactory.releasePythonWorker(python, env, first)
    val next = acquire()
    assert(next ne first)
    new DataOutputStream(next.getOutputStream).writeInt(123)
    assert(new DataInputStream(next.getInputStream).readInt() == 123)
  }

  test("worker handshake is bounded and negative fork results are rejected") {
    for (mode <- Seq("handshake_timeout", "negative_pid")) {
      val start = System.nanoTime()
      intercept[IOException] {
        acquire(conf + ("python.worker.startup.timeout" -> "200"), env + ("PYJAVA_FAULT" -> mode))
      }
      assert(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 5)
    }
  }

  test("a hung startup cannot block returns and borrows in another pool") {
    val first = acquire()
    val executor = Executors.newSingleThreadExecutor()
    val started = new java.util.concurrent.CountDownLatch(1)
    try {
      val hung = executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          started.countDown()
          intercept[IOException] {
            acquire(conf + ("python.worker.startup.timeout" -> "1500"),
              env + ("PYJAVA_FAULT" -> "silent"))
          }
        }
      })
      assert(started.await(2, TimeUnit.SECONDS))
      Thread.sleep(100)
      val start = System.nanoTime()
      PythonWorkerFactory.releasePythonWorker(python, env, first)
      assert(acquire() eq first)
      assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000)
      hung.get(5, TimeUnit.SECONDS)
    } finally executor.shutdownNow()
  }

  test("shutdown closes active sockets, and a stopped factory rejects new borrows") {
    val factory = new PythonWorkerFactory(python, env, conf)
    val socket = factory.create()
    factory.stop()
    assert(socket.isClosed)
    intercept[IllegalStateException](factory.create())
    factory.stop()
  }
}
