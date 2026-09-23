package tech.mlsql.arrow.python

import java.io._
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.{Callable, ExecutionException, FutureTask, TimeUnit, TimeoutException}

import tech.mlsql.arrow.Utils
import tech.mlsql.arrow.log.Logging
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Owns Python processes and their single-task-at-a-time sockets. */
class PythonWorkerFactory(pythonExec: String, envVars: Map[String, String], conf: Map[String, String])
  extends Logging {
  import PythonWorkerFactory.Tool._

  private val useDaemon = !System.getProperty("os.name").startsWith("Windows") &&
    conf.getOrElse(PYTHON_USE_DAEMON, "true").toBoolean
  private val daemonModule = conf.getOrElse(PYTHON_DAEMON_MODULE, "pyjava.daemon")
  private val workerModule = conf.getOrElse(PYTHON_WORKER_MODULE, "pyjava.worker")
  private val connectTimeout = positive(PYTHON_CONNECT_TIMEOUT, 10000)
  private val startupTimeout = positive(PYTHON_STARTUP_TIMEOUT, 10000)
  private val readTimeout = conf.getOrElse(PYTHON_SOCKET_TIMEOUT, "0").toInt
  require(readTimeout >= 0, s"$PYTHON_SOCKET_TIMEOUT must be non-negative")
  private val idleTimeout = TimeUnit.MINUTES.toNanos(positive(PYTHON_WORKER_IDLE_TIME, 1))
  private val daemonWorkers = new mutable.HashMap[Socket, Int]()
  private val simpleWorkers = new mutable.HashMap[Socket, Process]()
  private val leasedWorkers = new mutable.HashSet[Socket]()
  private val idleWorkers = new IdleWorkerPool(
    conf.getOrElse(PYTHON_WORKER_MAX_IDLE, "64").toInt, idleTimeout,
    positive(PYTHON_VALIDATE_TIMEOUT, 1), discardIdleWorker)

  private var daemon: Process = null
  val daemonHost: InetAddress = InetAddress.getByAddress(Array(127, 0, 0, 1))
  private var daemonPort = 0
  @volatile private var stopped = false
  private val pythonPath = mergePythonPaths(
    envVars.getOrElse("PYTHONPATH", ""), sys.env.getOrElse("PYTHONPATH", ""))

  private val monitorThread = new Thread(s"Idle Worker Monitor for $pythonExec") {
    setDaemon(true)
    override def run(): Unit = {
      try {
        while (!stopped) {
          Thread.sleep(10000)
          PythonWorkerFactory.this.synchronized { idleWorkers.evictExpired() }
        }
      } catch { case _: InterruptedException => () }
    }
  }
  monitorThread.start()

  private def positive(key: String, default: Int): Int = {
    val value = conf.getOrElse(key, default.toString).toInt
    require(value > 0, s"$key must be positive")
    value
  }

  def create(): Socket = synchronized {
    if (stopped) throw new IllegalStateException("Python worker factory is stopped")
    val socket = idleWorkers.borrow().getOrElse {
      if (useDaemon) createThroughDaemon() else createSimpleWorker()
    }
    leasedWorkers.add(socket)
    socket
  }

  private def configure(socket: Socket): Unit = {
    socket.setTcpNoDelay(true)
    socket.setKeepAlive(true)
    socket.setSoTimeout(readTimeout)
  }

  private def createThroughDaemon(): Socket = {
    def connect(): Socket = {
      val socket = new Socket()
      try {
        socket.connect(new InetSocketAddress(daemonHost, daemonPort), connectTimeout)
        socket.setSoTimeout(startupTimeout)
        val pid = new DataInputStream(socket.getInputStream).readInt()
        if (pid <= 0) throw new IOException(s"Python daemon failed to launch worker (code $pid)")
        configure(socket)
        daemonWorkers.put(socket, pid)
        socket
      } catch {
        case NonFatal(e) => closeSocket(socket); throw e
      }
    }

    startDaemon()
    try connect()
    catch {
      // A bad handshake must not kill other tasks in a healthy daemon.
      // Retry acquisition only, before any user code or data has been sent.
      case e: IOException if daemon != null && !daemon.isAlive =>
        logWarning("Python daemon exited; restarting before worker acquisition", e)
        stopDaemon()
        startDaemon()
        connect()
    }
  }

  private def processBuilder(module: String): ProcessBuilder = {
    val envCommand = envVars.getOrElse("PYTHON_ENV", "").trim
    def quote(value: String): String = "'" + value.replace("'", "'\"'\"'") + "'"
    val command = if (envCommand.isEmpty) Seq(pythonExec, "-m", module)
    else Seq("bash", "-c", envCommand + " && exec " + quote(pythonExec) + " -m " + quote(module))
    val pb = new ProcessBuilder(command.asJava)
    pb.environment().putAll(envVars.asJava)
    pb.environment().put("PYTHONPATH", pythonPath)
    pb.environment().put("PYTHONUNBUFFERED", "YES")
    pb
  }

  private def createSimpleWorker(): Socket = {
    val server = new ServerSocket(0, 1, daemonHost)
    var process: Process = null
    var socket: Socket = null
    try {
      server.setSoTimeout(startupTimeout)
      val pb = processBuilder(workerModule)
      pb.environment().put("PYTHON_WORKER_FACTORY_PORT", server.getLocalPort.toString)
      process = pb.start()
      Utils.redirectStream(conf, process.getInputStream)
      Utils.redirectStream(conf, process.getErrorStream)
      socket = server.accept()
      configure(socket)
      simpleWorkers.put(socket, process)
      socket
    } catch {
      case NonFatal(e) =>
        if (socket != null) closeSocket(socket)
        if (process != null) process.destroyForcibly()
        throw new IOException("Python worker failed to connect back", e)
    } finally { server.close() }
  }

  private def startDaemon(): Unit = {
    if (daemon != null && daemon.isAlive) return
    if (daemon != null) stopDaemon()
    try {
      daemon = processBuilder(daemonModule).start()
      // Drain stderr before the port handshake so imports cannot fill the pipe.
      Utils.redirectStream(conf, daemon.getErrorStream)
      val in = new DataInputStream(daemon.getInputStream)
      val portRead = new FutureTask[Int](new Callable[Int] {
        override def call(): Int = in.readInt()
      })
      val reader = new Thread(portRead, s"Python daemon startup for $pythonExec")
      reader.setDaemon(true)
      reader.start()
      try daemonPort = portRead.get(startupTimeout, TimeUnit.MILLISECONDS)
      catch {
        case e: TimeoutException =>
          portRead.cancel(true)
          throw new IOException(s"Python daemon did not announce a port within $startupTimeout ms", e)
        case e: ExecutionException =>
          throw new IOException(s"Cannot read port from $daemonModule; see worker stderr", e.getCause)
        case e: InterruptedException =>
          portRead.cancel(true)
          Thread.currentThread().interrupt()
          throw e
      }
      if (daemonPort < 1 || daemonPort > 65535)
        throw new IOException(s"Invalid Python daemon port $daemonPort; stdout must contain protocol data only")
      Utils.redirectStream(conf, in)
    } catch {
      case NonFatal(e) => stopDaemon(); throw e
      case e: InterruptedException => stopDaemon(); throw e
    }
  }

  private def closeSocket(socket: Socket): Unit = {
    try socket.close()
    catch { case NonFatal(e) => logWarning("Failed to close Python worker socket", e) }
  }

  private def discardIdleWorker(socket: Socket): Unit = {
    // Idle workers await their next request, so EOF releases them. Do not
    // signal a cached PID here: a dead worker's PID may have been reused.
    daemonWorkers.remove(socket)
    simpleWorkers.remove(socket).foreach(_.destroy())
    closeSocket(socket)
  }

  private def stopDaemon(): Unit = {
    idleWorkers.clear()
    daemonWorkers.keys.toList.foreach(closeSocket)
    daemonWorkers.clear()
    leasedWorkers.clear()
    if (daemon != null) daemon.destroy()
    daemon = null
    daemonPort = 0
  }

  def stop(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      monitorThread.interrupt()
      stopDaemon()
      simpleWorkers.foreach { case (socket, process) => closeSocket(socket); process.destroyForcibly() }
      simpleWorkers.clear()
    }
  }

  def stopWorker(worker: Socket): Unit = synchronized {
    idleWorkers.remove(worker)
    leasedWorkers.remove(worker)
    val pid = daemonWorkers.remove(worker)
    try {
      if (daemon != null && daemon.isAlive) pid.foreach { value =>
        val out = new DataOutputStream(daemon.getOutputStream)
        out.writeInt(value)
        out.flush()
      }
    } catch { case NonFatal(e) => logWarning("Cannot notify Python daemon of worker cancellation", e) }
    finally {
      simpleWorkers.remove(worker).foreach(_.destroyForcibly())
      closeSocket(worker)
    }
  }

  def releaseWorker(worker: Socket): Unit = synchronized {
    if (leasedWorkers.remove(worker)) {
      if (useDaemon && !stopped && daemon != null && daemon.isAlive) idleWorkers.release(worker)
      else discardIdleWorker(worker)
    }
  }
}

object PythonWorkerFactory {
  import Tool._
  private type Key = (String, Map[String, String], Map[String, String])
  private val pythonWorkers = new mutable.HashMap[Key, PythonWorkerFactory]()
  private val owners = new java.util.WeakHashMap[Socket, PythonWorkerFactory]()
  private val factoryOptions = Set(PYTHON_DAEMON_MODULE, PYTHON_WORKER_MODULE,
    PYTHON_USE_DAEMON, PYTHON_WORKER_IDLE_TIME, PYTHON_WORKER_MAX_IDLE,
    PYTHON_CONNECT_TIMEOUT, PYTHON_STARTUP_TIMEOUT, PYTHON_SOCKET_TIMEOUT,
    PYTHON_VALIDATE_TIMEOUT, REDIRECT_IMPL)

  def createPythonWorker(pythonExec: String, envVars: Map[String, String], conf: Map[String, String]): Socket = {
    val factory = synchronized {
      val key = (pythonExec, envVars, conf.filter { case (k, _) => factoryOptions.contains(k) })
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars, conf))
    }
    // Starting one environment must not block every other pool.
    val worker = factory.create()
    synchronized { owners.put(worker, factory) }
    worker
  }

  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket): Unit =
    destroyPythonWorker(worker)

  def destroyPythonWorker(worker: Socket): Unit = {
    val owner = synchronized { Option(owners.remove(worker)) }
    owner match {
      case Some(factory) => factory.stopWorker(worker)
      case None => worker.close()
    }
  }

  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket): Unit = {
    val owner = synchronized { Option(owners.get(worker)) }
    owner match {
      case Some(factory) => factory.releaseWorker(worker)
      case None => worker.close()
    }
  }

  /** Application shutdown; callers must finish or cancel their tasks first. */
  def shutdownAll(): Unit = {
    val factories = synchronized {
      val all = pythonWorkers.values.toList
      pythonWorkers.clear()
      owners.clear()
      all
    }
    factories.foreach(_.stop())
  }

  object Tool {
    val PROCESS_WAIT_TIMEOUT_MS = 10000
    val PYTHON_DAEMON_MODULE = "python.daemon.module"
    val PYTHON_WORKER_MODULE = "python.worker.module"
    val PYTHON_USE_DAEMON = "python.use.daemon"
    val PYTHON_WORKER_IDLE_TIME = "python.worker.idle.time"
    val PYTHON_WORKER_MAX_IDLE = "python.worker.pool.maxIdle"
    val PYTHON_CONNECT_TIMEOUT = "python.connect.timeout"
    val PYTHON_STARTUP_TIMEOUT = "python.worker.startup.timeout"
    val PYTHON_SOCKET_TIMEOUT = "python.socket.read.timeout"
    val PYTHON_VALIDATE_TIMEOUT = "python.worker.validate.timeout"
    val PYTHON_TASK_KILL_TIMEOUT = "python.task.killTimeout"
    val REDIRECT_IMPL = "python.redirect.impl"

    def mergePythonPaths(paths: String*): String = paths.filter(_ != "").mkString(File.pathSeparator)
  }
}
