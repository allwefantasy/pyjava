package tech.mlsql.arrow.python.runner

import org.apache.spark.SparkException
import tech.mlsql.arrow.log.Logging

import java.io._
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket, SocketTimeoutException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.SecureRandom
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Wire contract `pyjava-arrow-snapshot/1`, mirrored by pyjava/snapshot.py.
 *
 * Request:  int32 MAGIC | int32 VERSION | int32 OP | int32 tokenLen | token UTF-8
 * OP: 1=STATUS 2=READ 3=RELEASE
 * Response: int32 STATUS | int32 infoLen | UTF-8 JSON info
 * STATUS: 0=READY 1=PREPARING 2=FAILED 3=EXPIRED 4=UNKNOWN_TOKEN 5=BUSY 6=BAD_REQUEST
 * A READ whose status is READY appends: int64 length | snapshot bytes (legacy Arrow IPC).
 *
 * One connection may carry any number of sequential requests for different
 * tokens. Bare connect/close, malformed or incomplete handshakes and unknown
 * tokens never consume a snapshot. READ only transfers while state is ready.
 */
object SnapshotProtocol {
  val Magic: Int = 0x50594a53 // "PYJS"
  val Version: Int = 1
  val ProtocolName = "pyjava-arrow-snapshot/1"

  val OpStatus = 1
  val OpRead = 2
  val OpRelease = 3

  val StReady = 0
  val StPreparing = 1
  val StFailed = 2
  val StExpired = 3
  val StUnknown = 4
  val StBusy = 5
  val StBad = 6

  val MaxTokenBytes = 256
  val MaxInfoBytes = 65536

  def statusName(code: Int): String = code match {
    case StReady => "ready"
    case StPreparing => "preparing"
    case StFailed => "failed"
    case StExpired => "expired"
    case StUnknown => "unknown_token"
    case StBusy => "busy"
    case StBad => "bad_request"
    case other => s"unexpected($other)"
  }

  def writeRequest(out: DataOutputStream, op: Int, token: String): Unit = {
    val bytes = token.getBytes(StandardCharsets.UTF_8)
    out.writeInt(Magic)
    out.writeInt(Version)
    out.writeInt(op)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  def writeResponse(out: DataOutputStream, status: Int, info: String): Unit = {
    val bytes = if (info == null) Array.emptyByteArray else info.getBytes(StandardCharsets.UTF_8)
    out.writeInt(status)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  /** @return (op, token), or null when the peer closed before sending a frame. */
  def readRequest(in: DataInputStream): (Int, String) = {
    val magic = try in.readInt() catch {
      case _: EOFException => return null
    }
    if (magic != Magic) throw new IOException("bad snapshot frame magic")
    val version = in.readInt()
    if (version != Version) throw new IOException(s"unsupported snapshot protocol version $version")
    val op = in.readInt()
    val tokenLength = in.readInt()
    if (tokenLength <= 0 || tokenLength > MaxTokenBytes) {
      throw new IOException(s"invalid snapshot token length $tokenLength")
    }
    val token = new Array[Byte](tokenLength)
    in.readFully(token)
    (op, new String(token, StandardCharsets.UTF_8))
  }

  /** @return (status, info JSON) */
  def readResponse(in: DataInputStream): (Int, String) = {
    val status = in.readInt()
    val infoLength = in.readInt()
    if (infoLength < 0 || infoLength > MaxInfoBytes) {
      throw new IOException(s"invalid snapshot info length $infoLength")
    }
    val info = new Array[Byte](infoLength)
    in.readFully(info)
    (status, new String(info, StandardCharsets.UTF_8))
  }
}

/**
 * Engine-facing identity of one committed (or in-progress) partition snapshot.
 * Field order/names are the result-row contract; see PartitionDescriptor.FieldNames.
 */
final case class PartitionDescriptor(
                                      host: String,
                                      port: Int,
                                      token: String,
                                      protocol: String,
                                      partitionId: Long,
                                      attemptId: Long,
                                      snapshotBytes: Long,
                                      leaseDeadlineMs: Long,
                                      timezone: String) extends Serializable {
  def toArray: Array[Any] = Array(host, Int.box(port), token, protocol,
    Long.box(partitionId), Long.box(attemptId), Long.box(snapshotBytes),
    Long.box(leaseDeadlineMs), timezone)
}

object PartitionDescriptor {
  val FieldNames: Seq[String] = Seq("host", "port", "token", "protocol",
    "partition_id", "attempt_id", "snapshot_bytes", "lease_deadline_ms", "timezone")
}

object SnapshotEntry {
  val Preparing = 0
  val Ready = 1
  val Failed = 2
  val Expired = 3
}

/**
 * One registered snapshot attempt. Mutable only through the entry lock; state
 * and file fields are volatile so the status path never blocks on a reader.
 */
final class SnapshotEntry(val token: String,
                          val partitionId: Long,
                          val attemptId: Long,
                          val leaseMs: Long,
                          val prepareMs: Long,
                          val maxBytes: Long) extends Logging {
  import SnapshotEntry._

  private val lock = new Object
  @volatile private var state = Preparing
  @volatile private var path: Path = _
  @volatile private var sizeBytes = -1L
  @volatile private var error: String = _
  @volatile private var charged = 0L
  @volatile private var uncharged = false
  @volatile private var writerActive = false
  @volatile private var leaseDeadline = 0L
  @volatile private var goneAfter = Long.MaxValue
  private var readers = 0
  private var deletePending = false
  private var fileDeleted = false
  val registeredMs: Long = System.currentTimeMillis()
  val prepareDeadlineMs: Long = registeredMs + prepareMs

  def currentState: Int = state

  def openReaders: Int = lock.synchronized(readers)

  def size: Long = sizeBytes

  def leaseDeadlineMs: Long = leaseDeadline

  def snapshotPath: Path = path

  def chargedBytes: Long = charged

  def attachFile(snapshot: Path): Unit = lock.synchronized {
    path = snapshot
  }

  def beginWrite(): Unit = lock.synchronized {
    if (state != Preparing) throw new IOException("snapshot generation cancelled")
    writerActive = true
  }

  /**
   * Charge ``n`` bytes or refuse. A release, failure or prepare deadline stops
   * the writer here so a cancelled attempt cannot keep growing the file.
   */
  def noteWrite(n: Long, charge: Long => Unit): Unit = lock.synchronized {
    if (state != Preparing) throw new IOException("snapshot generation cancelled")
    if (System.currentTimeMillis() > prepareDeadlineMs) {
      throw new IOException("snapshot generation exceeded python.socket.shared.prepare.timeout.ms")
    }
    if (charged + n > maxBytes) {
      throw new IOException("Partition exceeds python.socket.shared.partition.maxBytes")
    }
    charge(n)
    charged += n
  }

  def markReady(snapshot: Path, size: Long): Unit = lock.synchronized {
    writerActive = false
    path = snapshot
    if (state != Preparing) throw new IOException("snapshot generation cancelled")
    sizeBytes = size
    state = Ready
    leaseDeadline = System.currentTimeMillis() + leaseMs
  }

  /** Failure is sticky: a failed attempt stays queryable for its lease window. */
  def fail(message: String): Unit = lock.synchronized {
    if (state == Preparing || state == Ready) {
      state = Failed
      error = if (message == null) "unknown" else message.take(2048)
      if (leaseDeadline == 0) leaseDeadline = System.currentTimeMillis() + leaseMs
      if (!writerActive) deleteFileLocked()
    }
  }

  def failIfPreparing(message: String): Unit = lock.synchronized {
    if (state == Preparing) fail(message)
  }

  /** Writer is leaving, successfully or not. Deletes unless a reader holds the file. */
  def abortWrite(message: String): Unit = lock.synchronized {
    writerActive = false
    if (state == Preparing) {
      state = Failed
      error = if (message == null) "unknown" else message.take(2048)
      if (leaseDeadline == 0) leaseDeadline = System.currentTimeMillis() + leaseMs
    }
    deleteFileLocked()
  }

  def expire(graceMs: Long): Unit = lock.synchronized {
    if (state != Expired) {
      state = Expired
      goneAfter = System.currentTimeMillis() + graceMs
      if (!writerActive) deleteFileLocked()
    }
  }

  private def deleteFileLocked(): Unit = {
    if (path != null && !fileDeleted) {
      if (readers > 0) deletePending = true
      else {
        try Files.deleteIfExists(path)
        catch {
          case NonFatal(e) => logWarning(s"failed to delete snapshot file $path", e)
        }
        fileDeleted = true
      }
    }
  }

  /**
   * Bytes to subtract from the process budget, once the file is gone.
   * A live writer or reader keeps the charge so release cannot free budget
   * for bytes that are still on disk. A second call returns 0.
   */
  def releaseCharge(): Long = lock.synchronized {
    if (uncharged || writerActive || readers > 0) 0L
    else if (path != null && !fileDeleted) 0L
    else {
      uncharged = true
      val amount = charged
      charged = 0L
      amount
    }
  }

  /** @return an open reader for the immutable file, or null if not ready. */
  def openReader(): InputStream = lock.synchronized {
    if (state != Ready || path == null || fileDeleted) return null
    readers += 1
    try new FileInputStream(path.toFile)
    catch {
      case NonFatal(e) =>
        readers -= 1
        throw e
    }
  }

  def closeReader(): Unit = lock.synchronized {
    if (readers > 0) readers -= 1
    if (deletePending && readers <= 0 && !fileDeleted) {
      try Files.deleteIfExists(path)
      catch {
        case NonFatal(e) => logWarning(s"failed to delete snapshot file $path", e)
      }
      fileDeleted = true
    }
  }

  def preparingTooLong(nowMs: Long): Boolean = state == Preparing && nowMs > prepareDeadlineMs

  def leaseTimedOut(nowMs: Long): Boolean =
    (state == Ready || state == Failed) && leaseDeadline > 0 && nowMs > leaseDeadline

  def removablyExpired(nowMs: Long): Boolean = state == Expired && nowMs > goneAfter

  private def jsonEscape(value: String): String = value.flatMap {
    case '"' => "\\\""
    case '\\' => "\\\\"
    case c if c < 0x20 => f"\\u${c.toInt}%04x"
    case c => c.toString
  }

  def infoJson: String = lock.synchronized {
    val stateName = state match {
      case Preparing => "preparing"
      case Ready => "ready"
      case Failed => "failed"
      case Expired => "expired"
    }
    val base = new StringBuilder(160)
    base.append("{\"state\":\"").append(stateName).append('"')
      .append(",\"partitionId\":").append(partitionId)
      .append(",\"attemptId\":").append(attemptId)
      .append(",\"snapshotBytes\":").append(sizeBytes)
      .append(",\"leaseDeadlineMs\":").append(leaseDeadline)
    if (error != null) base.append(",\"error\":\"").append(jsonEscape(error)).append('"')
    base.append('}').toString
  }

  def statusCode: Int = state match {
    case Preparing => SnapshotProtocol.StPreparing
    case Ready => SnapshotProtocol.StReady
    case Failed => SnapshotProtocol.StFailed
    case Expired => SnapshotProtocol.StExpired
  }
}

/**
 * One process-level listener serving many token-scoped partition snapshots.
 * Instances are shared per (name, host, port) via [[ArrowSnapshotServices]].
 */
final class ArrowSnapshotService(val bindHost: String, conf: Map[String, String])
  extends Logging {
  import SnapshotProtocol._

  private def positive(key: String, default: Long): Long = {
    val raw = conf.getOrElse(key, default.toString).toLong
    require(raw > 0, s"$key must be positive, got $raw")
    raw
  }

  val maxSnapshots: Int = positive("python.socket.shared.maxSnapshots", 256).toInt
  val totalMaxBytes: Long = positive("python.socket.shared.total.maxBytes", 4294967296L)
  val maxConnections: Int = positive("python.socket.shared.maxConnections", 64).toInt
  val handshakeTimeoutMs: Int = positive("python.socket.shared.handshake.timeout", 10000).toInt
  val writeTimeoutMs: Int = conf.getOrElse("python.socket.write.timeout", "300000").toInt
  val leaseMs: Long = positive("python.socket.shared.lease.ms", 1800000)
  val prepareMs: Long = positive("python.socket.shared.prepare.timeout.ms", leaseMs)
  val expiredGraceMs: Long = positive("python.socket.shared.expired.grace.ms", 60000)
  private val bufferSize = ArrowSockets.bufferSize(conf)
  private val fixedPort = conf.getOrElse("python.socket.shared.port", "0").toInt
  val advertiseHost: String = conf.getOrElse("python.socket.shared.advertise.host", bindHost)

  private val server = new ServerSocket()
  server.setReuseAddress(true)
  server.bind(new InetSocketAddress(InetAddress.getByName(bindHost), fixedPort), 64)
  val port: Int = server.getLocalPort

  private val registry = new ConcurrentHashMap[String, SnapshotEntry]()
  private val totalBytes = new AtomicLong(0)
  private val connSlots = new Semaphore(maxConnections)
  private val activeSockets = java.util.Collections.newSetFromMap(
    new ConcurrentHashMap[Socket, java.lang.Boolean]())
  private val closed = new AtomicBoolean(false)
  private val budgetSignature: Map[String, String] = Map(
    "python.socket.shared.total.maxBytes" -> totalMaxBytes.toString,
    "python.socket.shared.maxSnapshots" -> maxSnapshots.toString,
    "python.socket.shared.maxConnections" -> maxConnections.toString
  )
  private val pool = new ThreadPoolExecutor(0, maxConnections, 60L, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](), daemonFactory("pyjava-snapshot-conn"),
    new ThreadPoolExecutor.AbortPolicy())

  private val reaper = new ScheduledThreadPoolExecutor(1, daemonFactory("pyjava-snapshot-reaper"))
  reaper.setRemoveOnCancelPolicy(true)

  private val acceptThread = new Thread("pyjava-snapshot-accept") {
    setDaemon(true)

    override def run(): Unit = {
      while (!closed.get()) {
        try {
          val socket = server.accept()
          if (!connSlots.tryAcquire()) {
            try {
              val out = new DataOutputStream(socket.getOutputStream)
              writeResponse(out, StBusy, "connection limit")
              out.flush()
            } catch {
              case NonFatal(_) =>
            } finally ArrowSockets.closeQuietly(socket)
          } else {
            activeSockets.add(socket)
            try pool.execute(connectionHandler(socket))
            catch {
              case NonFatal(e) =>
                activeSockets.remove(socket)
                connSlots.release()
                ArrowSockets.closeQuietly(socket)
                logWarning("snapshot connection handler rejected", e)
            }
          }
        } catch {
          case e: IOException if closed.get() || server.isClosed => return
          case NonFatal(e) => logWarning("snapshot accept failed", e)
        }
      }
    }
  }
  acceptThread.start()
  reaper.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = try reap() catch {
      case NonFatal(e) => logWarning("snapshot reaper failed", e)
    }
  }, 500, 500, TimeUnit.MILLISECONDS)

  private def daemonFactory(name: String): ThreadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, name)
      thread.setDaemon(true)
      thread
    }
  }

  def newSpoolFile(conf: Map[String, String]): Path = conf.get("python.socket.shared.dir") match {
    case Some(dir) =>
      val directory = java.nio.file.Paths.get(dir)
      Files.createDirectories(directory)
      Files.createTempFile(directory, "pyjava-snapshot-", ".arrow")
    case None => Files.createTempFile("pyjava-snapshot-", ".arrow")
  }

  // ---- quota ----------------------------------------------------------
  def charge(bytes: Long): Unit = {
    val total = totalBytes.addAndGet(bytes)
    if (total > totalMaxBytes) {
      totalBytes.addAndGet(-bytes)
      throw new IOException("process snapshot byte budget exceeded " +
        "(python.socket.shared.total.maxBytes)")
    }
  }

  def uncharge(bytes: Long): Unit = {
    if (bytes == 0) return
    if (bytes < 0) throw new IllegalStateException(s"negative snapshot uncharge: $bytes")
    var current = totalBytes.get()
    while (true) {
      if (current < bytes) {
        throw new IllegalStateException(
          s"snapshot budget would become negative ($current - $bytes)")
      }
      if (totalBytes.compareAndSet(current, current - bytes)) return
      current = totalBytes.get()
    }
  }

  private def settle(entry: SnapshotEntry): Unit = settleQuiet(entry)

  def settleQuiet(entry: SnapshotEntry): Unit = {
    val bytes = entry.releaseCharge()
    if (bytes > 0) uncharge(bytes)
  }

  def usedBytes: Long = totalBytes.get()

  def registeredCount: Int = registry.size()

  // ---- producer API -----------------------------------------------------
  def register(partitionId: Long, attemptId: Long, conf: Map[String, String]): SnapshotEntry = {
    if (closed.get()) throw new SparkException("shared snapshot service is closed")
    if (registry.size() >= maxSnapshots) {
      throw new SparkException("shared snapshot registry is full " +
        s"(python.socket.shared.maxSnapshots=$maxSnapshots)")
    }
    val token = randomToken()
    val requestedLease = conf.getOrElse("python.socket.shared.lease.ms", leaseMs.toString).toLong
    val requestedPrepare = conf.getOrElse("python.socket.shared.prepare.timeout.ms", prepareMs.toString).toLong
    val requestedMax = conf.getOrElse("python.socket.shared.partition.maxBytes",
      conf.getOrElse("python.socket.spool.maxBytes", "1073741824")).toLong
    require(requestedLease > 0, "python.socket.shared.lease.ms must be positive")
    require(requestedPrepare > 0, "python.socket.shared.prepare.timeout.ms must be positive")
    require(requestedMax > 0, "python.socket.shared.partition.maxBytes must be positive")
    val entry = new SnapshotEntry(token, partitionId, attemptId,
      requestedLease, requestedPrepare, requestedMax)
    registry.synchronized {
      if (registry.size() >= maxSnapshots) {
        throw new SparkException("shared snapshot registry is full " +
          s"(python.socket.shared.maxSnapshots=$maxSnapshots)")
      }
      registry.put(token, entry)
    }
    entry
  }

  private def randomToken(): String = {
    val bytes = new Array[Byte](16)
    ArrowSnapshotService.random.nextBytes(bytes)
    bytes.map("%02x".format(_)).mkString
  }

  def entry(token: String): Option[SnapshotEntry] = Option(registry.get(token))

  def release(token: String): Boolean = {
    entry(token).exists { e =>
      e.expire(expiredGraceMs)
      settle(e)
      true
    }
  }

  def tokens: Set[String] = registry.keySet().asScala.toSet

  // ---- listener ---------------------------------------------------------
  private def connectionHandler(socket: Socket): Runnable = new Runnable {
    override def run(): Unit = {
      var reader: InputStream = null
      var entry: SnapshotEntry = null
      try {
        ArrowSockets.prepare(socket, bufferSize)
        val in = new DataInputStream(new BufferedInputStream(socket.getInputStream, 65536))
        val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
        var done = false
        while (!done && !closed.get()) {
          socket.setSoTimeout(handshakeTimeoutMs)
          val request = try readRequest(in)
          catch {
            case _: SocketTimeoutException | _: EOFException => null
          }
          if (request == null) {
            done = true
          } else {
            val (op, token) = request
            registry.get(token) match {
              case null =>
                writeResponse(out, StUnknown, null)
                out.flush()
              case registered =>
                entry = registered
                op match {
                  case `OpStatus` =>
                    writeResponse(out, entry.statusCode, entry.infoJson)
                    out.flush()
                  case `OpRelease` =>
                    release(token)
                    writeResponse(out, StReady, null)
                    out.flush()
                  case `OpRead` =>
                    entry.statusCode match {
                      case StReady =>
                        reader = entry.openReader()
                        if (reader == null) {
                          writeResponse(out, entry.statusCode, entry.infoJson)
                          out.flush()
                        } else {
                          writeResponse(out, StReady, entry.infoJson)
                          out.writeLong(entry.size)
                          out.flush()
                          streamToClient(socket, reader, out)
                          entry.closeReader()
                          settle(entry)
                          reader = null
                        }
                      case code =>
                        writeResponse(out, code, entry.infoJson)
                        out.flush()
                    }
                  case _ =>
                    writeResponse(out, StBad, null)
                    out.flush()
                    done = true
                }
            }
          }
        }
      } catch {
        case _: SocketTimeoutException | _: EOFException | _: IOException => // peer went away
        case NonFatal(e) => logWarning("shared snapshot connection failed", e)
      } finally {
        if (reader != null) {
          try reader.close()
          catch {
            case NonFatal(_) =>
          }
          if (entry != null) {
            entry.closeReader()
            settle(entry)
          }
        }
        activeSockets.remove(socket)
        ArrowSockets.closeQuietly(socket)
        connSlots.release()
      }
    }
  }

  /** Copy the immutable snapshot file to the client with an idle-write bound. */
  private def streamToClient(socket: Socket, reader: InputStream, out: OutputStream): Unit = {
    val activity = new AtomicLong(System.nanoTime())
    val guard = ArrowSockets.watch(socket, None, writeTimeoutMs, activity)
    val buffer = new Array[Byte](bufferSize)
    try {
      var count = reader.read(buffer)
      while (count >= 0) {
        out.write(buffer, 0, count)
        activity.set(System.nanoTime())
        count = reader.read(buffer)
      }
      out.flush()
    } finally {
      guard.cancel(false)
      try reader.close()
      catch {
        case NonFatal(_) =>
      }
    }
  }

  private def reap(): Unit = {
    val now = System.currentTimeMillis()
    registry.values().asScala.foreach { entry =>
      if (entry.preparingTooLong(now)) {
        entry.fail("snapshot generation exceeded python.socket.shared.prepare.timeout.ms")
        settle(entry)
      }
      if (entry.leaseTimedOut(now)) {
        entry.expire(expiredGraceMs)
        settle(entry)
      }
      if (entry.removablyExpired(now)) {
        registry.remove(entry.token, entry)
        settle(entry)
      }
    }
  }

  def shutdown(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try server.close()
      catch {
        case NonFatal(_) =>
      }
      // Close accepted sockets now. Do not wait out a read timeout.
      activeSockets.asScala.foreach(ArrowSockets.closeQuietly)
      registry.values().asScala.foreach { entry =>
        entry.expire(0)
        settle(entry)
        registry.remove(entry.token, entry)
      }
      pool.shutdownNow()
      reaper.shutdownNow()
    }
  }

  def isClosed: Boolean = closed.get()

  /**
   * Process budgets are fixed by the first open of this (name, host, port).
   * A later call that sets a different value is rejected so callers cannot
   * believe a lower cap took effect.
   */
  def assertCompatible(conf: Map[String, String]): Unit = {
    budgetSignature.foreach { case (key, effective) =>
      conf.get(key).foreach { requested =>
        if (requested != effective) {
          throw new SparkException(
            s"shared snapshot service on $bindHost:$port was already configured with " +
              s"$key=$effective; refusing conflicting $key=$requested. These budgets are " +
              "process-level and fixed by the first request. Use a different " +
              "python.socket.shared.name or the same values.")
        }
      }
    }
  }
}

object ArrowSnapshotService {
  private val random = new SecureRandom()
}

/** Per-process registry of shared snapshot services. */
object ArrowSnapshotServices extends Logging {
  private val services = new ConcurrentHashMap[String, ArrowSnapshotService]()
  private val hookInstalled = new AtomicBoolean(false)

  def get(host: String, conf: Map[String, String]): ArrowSnapshotService = {
    installShutdownHook()
    val key = conf.getOrElse("python.socket.shared.name", "default") + "|" + host +
      "|" + conf.getOrElse("python.socket.shared.port", "0")
    services.compute(key, (_, existing) => {
      if (existing == null || existing.isClosed) new ArrowSnapshotService(host, conf)
      else {
        existing.assertCompatible(conf)
        existing
      }
    })
  }

  def shutdownAll(): Unit = {
    services.values().asScala.foreach(_.shutdown())
    services.clear()
  }

  private def installShutdownHook(): Unit = {
    if (hookInstalled.compareAndSet(false, true)) {
      Runtime.getRuntime.addShutdownHook(new Thread("pyjava-snapshot-shutdown") {
        override def run(): Unit = shutdownAll()
      })
    }
  }
}

/** Input stream bounded to the announced payload length; short reads fail. */
final class BoundedSnapshotInput(in: InputStream, val length: Long,
                                 activity: AtomicLong) extends InputStream {
  private var remaining = length
  private var consumed = 0L

  def consumedBytes: Long = consumed

  override def read(): Int = {
    if (remaining <= 0) -1
    else {
      val value = in.read()
      if (value < 0) throw new EOFException("truncated shared snapshot payload")
      consumed += 1
      remaining -= 1
      activity.set(System.nanoTime())
      value
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (remaining <= 0) -1
    else {
      val want = math.min(len.toLong, remaining).toInt
      var got = 0
      while (got < want) {
        val n = in.read(b, off + got, want - got)
        if (n < 0) throw new EOFException("truncated shared snapshot payload")
        got += n
      }
      consumed += got
      remaining -= got
      activity.set(System.nanoTime())
      got
    }
  }

  /** Drain any trailing bytes, then require the announced length was read. */
  def expectEnd(): Unit = {
    val buffer = new Array[Byte](65536)
    while (remaining > 0) {
      val n = read(buffer, 0, math.min(remaining, buffer.length.toLong).toInt)
      if (n < 0) throw new EOFException("truncated shared snapshot payload")
    }
    if (consumed != length) {
      throw new IOException(s"snapshot payload length mismatch: $consumed != $length")
    }
  }
}
