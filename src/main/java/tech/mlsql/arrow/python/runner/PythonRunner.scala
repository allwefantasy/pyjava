/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.arrow.python.runner

import java.io._
import java.net._
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Map => JMap}

import org.apache.spark._
import tech.mlsql.arrow.Utils
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str
import tech.mlsql.common.utils.lang.sc.ScalaReflect
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object PythonConf {
  val BUFFER_SIZE = "buffer_size"
  val PY_WORKER_REUSE = "py_worker_reuse"
  val PY_EXECUTOR_MEMORY = "py_executor_memory"
  val EXECUTOR_CORES = "executor_cores"
  val PYTHON_ENV = "python_env"
  val PY_INTERACTIVE = "no"
  val PY_EXECUTE_USER = "no"
}

case class PythonFunction(
                           command: String,
                           envVars: JMap[String, String],
                           pythonExec: String,
                           pythonVer: String)

case class ChainedPythonFunctions(funcs: Seq[PythonFunction])

abstract class BasePythonRunner[IN, OUT](
                                          funcs: Seq[ChainedPythonFunctions],
                                          conf: Map[String, String]
                                        )
  extends Logging {

  import PythonConf._


  protected val bufferSize: Int = conf.getOrElse(BUFFER_SIZE, "65536").toInt
  private val reuseWorker = conf.getOrElse(PY_WORKER_REUSE, "true").toBoolean
  // each python worker gets an equal part of the allocation. the worker pool will grow to the
  // number of concurrent tasks, which is determined by the number of cores in this executor.
  private val memoryMb = conf.get(PY_EXECUTOR_MEMORY).map(_.toInt / conf.getOrElse(EXECUTOR_CORES, "1").toInt)

  // All the Python functions should have the same exec, version and envvars.
  protected val envVars: java.util.Map[String, String] = funcs.head.funcs.head.envVars
  protected val pythonExec: String = funcs.head.funcs.head.pythonExec
  protected val pythonVer: String = funcs.head.funcs.head.pythonVer


  // Expose a ServerSocket to support method calls via socket from Python side.
  var serverSocket: Option[ServerSocket] = None


  def compute(
               inputIterator: Iterator[IN],
               partitionIndex: Int,
               commonTaskContext: CommonTaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis

    if (reuseWorker) {
      envVars.put(str(PY_WORKER_REUSE), "1")
    }
    if (memoryMb.isDefined) {
      envVars.put(str(PY_EXECUTOR_MEMORY), memoryMb.get.toString)
    }
    envVars.put(str(BUFFER_SIZE), bufferSize.toString)
    val worker: Socket = PythonWorkerFactory.createPythonWorker(pythonExec, envVars.asScala.toMap, conf)
    // Whether is the worker released into idle pool or closed. When any codes try to release or
    // close a worker, they should use `releasedOrClosed.compareAndSet` to flip the state to make
    // sure there is only one winner that is going to release or close the worker.
    val releasedOrClosed = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = newWriterThread(worker, inputIterator, partitionIndex, commonTaskContext)

    commonTaskContext.pythonWorkerRegister(() => {
      writerThread.shutdownOnTaskCompletion()
    })(releasedOrClosed, reuseWorker, worker)

    writerThread.start()
    new MonitorThread(worker, commonTaskContext, conf).start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

    val stdoutIterator = newReaderIterator(
      stream, writerThread, startTime, worker, releasedOrClosed, commonTaskContext)
    new InterruptibleIterator(commonTaskContext, stdoutIterator)
  }

  protected def newWriterThread(
                                 worker: Socket,
                                 inputIterator: Iterator[IN],
                                 partitionIndex: Int,
                                 context: CommonTaskContext): WriterThread

  protected def newReaderIterator(
                                   stream: DataInputStream,
                                   writerThread: WriterThread,
                                   startTime: Long,
                                   worker: Socket,
                                   releasedOrClosed: AtomicBoolean,
                                   context: CommonTaskContext): Iterator[OUT]

  /**
    * The thread responsible for writing the data from the PythonRDD's parent iterator to the
    * Python process.
    */
  abstract class WriterThread(
                               worker: Socket,
                               inputIterator: Iterator[IN],
                               partitionIndex: Int,
                               context: CommonTaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Throwable = null

    setDaemon(true)

    /** Contains the throwable thrown while writing the parent iterator to the Python process. */
    def exception: Option[Throwable] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      context.assertTaskIsCompleted(() => {})
      this.interrupt()
    }

    /**
      * Writes a command section to the stream connected to the Python worker.
      */
    protected def writeCommand(dataOut: DataOutputStream): Unit

    /**
      * Writes input data to the stream connected to the Python worker.
      */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        context.setTaskContext()
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)

        // Init a ServerSocket to accept method calls from Python side.
        val isBarrier = context.isBarrier
        if (isBarrier) {
          serverSocket = Some(new ServerSocket(/* port */ 0,
            /* backlog */ 1,
            InetAddress.getByName("localhost")))
          // A call to accept() for ServerSocket shall block infinitely.
          serverSocket.map(_.setSoTimeout(0))
          new Thread("accept-connections") {
            setDaemon(true)

            override def run(): Unit = {
              while (!serverSocket.get.isClosed()) {
                var sock: Socket = null
                try {
                  sock = serverSocket.get.accept()
                  // Wait for function call from python side.
                  sock.setSoTimeout(10000)
                  val input = new DataInputStream(sock.getInputStream())
                  input.readInt() match {
                    case BarrierTaskContextMessageProtocol.BARRIER_FUNCTION =>
                      // The barrier() function may wait infinitely, socket shall not timeout
                      // before the function finishes.
                      sock.setSoTimeout(0)
                      barrierAndServe(sock)

                    case _ =>
                      val out = new DataOutputStream(new BufferedOutputStream(
                        sock.getOutputStream))
                      writeUTF(BarrierTaskContextMessageProtocol.ERROR_UNRECOGNIZED_FUNCTION, out)
                  }
                } catch {
                  case e: SocketException if e.getMessage.contains("Socket closed") =>
                  // It is possible that the ServerSocket is not closed, but the native socket
                  // has already been closed, we shall catch and silently ignore this case.
                } finally {
                  if (sock != null) {
                    sock.close()
                  }
                }
              }
            }
          }.start()
        }

        // Close ServerSocket on task completion.
        serverSocket.foreach { server =>
          context.javaSideSocketServerRegister()(server)
        }
        val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
        if (boundPort == -1) {
          val message = "ServerSocket failed to bind to Java side."
          logError(message)
          throw new SparkException(message)
        } else if (isBarrier) {
          logDebug(s"Started ServerSocket on port $boundPort.")
        }

        // all information we will write
        // Notice that we should in python side get these information
        dataOut.writeBoolean(isBarrier)
        dataOut.writeInt(boundPort)
        writeCommand(dataOut)
        writeIteratorToStream(dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case t: Throwable if (NonFatal(t) || t.isInstanceOf[Exception]) =>
          if (context.isTaskCompleteOrInterrupt()()) {
            logDebug("Exception/NonFatal Error thrown after task completion (likely due to " +
              "cleanup)", t)
            if (!worker.isClosed) {
              Utils.tryLog(worker.shutdownOutput())
            }
          } else {
            // We must avoid throwing exceptions/NonFatals here, because the thread uncaught
            // exception handler will kill the whole executor (see
            // org.apache.spark.executor.Executor).
            _exception = t
            if (!worker.isClosed) {
              Utils.tryLog(worker.shutdownOutput())
            }
          }
      }
    }

    /**
      * Gateway to call BarrierTaskContext.barrier().
      */
    def barrierAndServe(sock: Socket): Unit = {
      require(serverSocket.isDefined, "No available ServerSocket to redirect the barrier() call.")

      val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
      try {
        ScalaReflect.fromInstance(context.innerContext).method("barrier").invoke()
        writeUTF(BarrierTaskContextMessageProtocol.BARRIER_RESULT_SUCCESS, out)
      } catch {
        case e: SparkException =>
          writeUTF(e.getMessage, out)
      } finally {
        out.close()
      }
    }

    def writeUTF(str: String, dataOut: DataOutputStream) {
      val bytes = str.getBytes(UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }
  }

  abstract class ReaderIterator(
                                 stream: DataInputStream,
                                 writerThread: WriterThread,
                                 startTime: Long,
                                 worker: Socket,
                                 releasedOrClosed: AtomicBoolean,
                                 context: CommonTaskContext)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    private var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
      * Reads next object from the stream.
      * When the stream reaches end of data, needs to process the following sections,
      * and then returns null.
      */
    protected def read(): OUT


    protected def handlePythonException(): SparkException = {
      // Signals that an exception has been thrown in python
      val exLength = stream.readInt()
      val obj = new Array[Byte](exLength)
      stream.readFully(obj)
      new SparkException(new String(obj, StandardCharsets.UTF_8),
        writerThread.exception.getOrElse(null))
    }

    protected def handleEndOfStream(): Unit = {
      if (reuseWorker && releasedOrClosed.compareAndSet(false, true)) {
        PythonWorkerFactory.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
      }
      eos = true
    }

    protected def handleEndOfDataSection(): Unit = {
      val flag = stream.readInt()
      if (flag == SpecialLengths.END_OF_STREAM) {
        if (reuseWorker && releasedOrClosed.compareAndSet(false, true)) {
          PythonWorkerFactory.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
        }
      } else {
        logWarning(
          s"""
             |-----------------------WARNING--------------------------------------------------------------------
             |Here we should received message is SpecialLengths.END_OF_STREAM:${SpecialLengths.END_OF_STREAM}
             |But It's now ${flag}.
             |This may cause the **** python worker leak **** and make the ***interactive mode fails***.
             |--------------------------------------------------------------------------------------------------
           """.stripMargin)
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception if context.isTaskInterrupt()() =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException(context.getTaskKillReason()().getOrElse("unknown reason"))

      case e: Exception if writerThread.exception.isDefined =>
        logError("Python worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", writerThread.exception.get)
        throw writerThread.exception.get

      case eof: EOFException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
    }
  }

  /**
    * It is necessary to have a monitor thread for python workers if the user cancels with
    * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
    * threads can block indefinitely.
    */
  class MonitorThread(worker: Socket, context: CommonTaskContext, conf: Map[String, String])
    extends Thread(s"Worker Monitor for $pythonExec") {

    /** How long to wait before killing the python worker if a task cannot be interrupted. */
    private val taskKillTimeout = conf.getOrElse(PythonWorkerFactory.Tool.PYTHON_TASK_KILL_TIMEOUT, "20000").toLong

    setDaemon(true)

    override def run() {
      context.monitor(() => {})(taskKillTimeout, pythonExec, envVars.asScala.toMap, worker)
    }
  }

}


object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
  val READ_SCHEMA = -7
}

object BarrierTaskContextMessageProtocol {
  val BARRIER_FUNCTION = 1
  val BARRIER_RESULT_SUCCESS = "success"
  val ERROR_UNRECOGNIZED_FUNCTION = "Not recognized function call from python side."
}

class InterruptibleIterator[+T](val context: CommonTaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  def hasNext: Boolean = {
    // TODO(aarondav/rxin): Check Thread.interrupted instead of context.interrupted if interrupt
    // is allowed. The assumption is that Thread.interrupted does not have a memory fence in read
    // (just a volatile field in C), while context.interrupted is a volatile in the JVM, which
    // introduces an expensive read fence.
    context.killTaskIfInterrupted()
    delegate.hasNext
  }

  def next(): T = delegate.next()
}

