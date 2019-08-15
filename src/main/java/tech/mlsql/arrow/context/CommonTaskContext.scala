package tech.mlsql.arrow.context

import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import tech.mlsql.arrow.python.runner.ArrowPythonRunner

/**
  * This class is used to manager your tasks. When you create a thread to execute something which uses ArrowPythonRunner
  * , we should take care the situation when the task is normally completed or unexpectedly finished, there are some callback or action
  * should be performed
  */
trait CommonTaskContext {
  val arrowPythonRunner: ArrowPythonRunner

  /**
    * When reader begins to read, it will invoke the return function. Please use something to remember them
    * and When the task is done, use the returned function to clean the reader resource
    */
  def readerRegister(callback: () => Unit): (ArrowStreamReader, BufferAllocator) => Unit

  /**
    * releasedOrClosed:AtomicBoolean should release or close the python worker we have used.
    * reuseWorker: should reuse the python worker
    * socket: python worker
    * When task started, it will invoke the return function. Please use something to remember them and
    * 2hen the task is done, use the returned function to clean the python resource
    */
  def pythonWorkerRegister(callback: () => Unit): (
    AtomicBoolean,
      Boolean,
      Socket) => Unit


  /**
    *
    * isBarrier is true, you should also set shutdownServerSocket
    */
  def isBarrier: Boolean

  /**
    * When task started, it will invoke the return function. Please use something to remember them and
    * when the task is done, use the returned function to clean the python server side SocketServer
    */
  def javaSideSocketServerRegister(): (ServerSocket) => Unit

  def monitor(callback: () => Unit): (Long, String, Map[String, String], Socket) => Unit

  def assertTaskIsCompleted(callback: () => Unit): () => Unit

  /**
    *
    * set innerContext, and you can use innerContext to get it again
    */
  def setTaskContext(): () => Unit

  def innerContext: Any

  /**
    * check task status
    */
  def isTaskCompleteOrInterrupt(): () => Boolean

  def isTaskInterrupt(): () => Boolean

  def getTaskKillReason(): () => Option[String]

  def killTaskIfInterrupted(): () => Unit
}
