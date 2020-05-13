package tech.mlsql.arrow.python.iapp

import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{EventListener, UUID}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.runner.ArrowPythonRunner
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-08-15 WilliamZhu(allwefantasy@gmail.com)
  */

class AppContextImpl(context: JavaContext, _arrowPythonRunner: ArrowPythonRunner) extends CommonTaskContext with Logging {
  override def pythonWorkerRegister(callback: () => Unit) = {
    (releasedOrClosed: AtomicBoolean,
     reuseWorker: Boolean,
     worker: Socket
    ) => {
      context.addTaskCompletionListener[Unit] { _ =>
        //writerThread.shutdownOnTaskCompletion()
        callback()
        if (!reuseWorker || releasedOrClosed.compareAndSet(false, true)) {
          try {
            worker.close()
          } catch {
            case e: Exception =>
              logWarning("Failed to close worker socket", e)
          }
        }
      }
    }
  }

  override def assertTaskIsCompleted(callback: () => Unit) = {
    () => {
      assert(context.isCompleted)
    }
  }

  override def setTaskContext(): () => Unit = {
    () => {

    }
  }

  override def innerContext: Any = context

  override def isBarrier: Boolean = false

  override def monitor(callback: () => Unit) = {
    (taskKillTimeout: Long, pythonExec: String, envVars: Map[String, String], worker: Socket) => {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted && !context.isCompleted) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted) {
        Thread.sleep(taskKillTimeout)
        if (!context.isCompleted) {
          try {
            // Mimic the task name used in `Executor` to help the user find out the task to blame.
            val taskName = s"${context.partitionId}"
            logWarning(s"Incomplete task $taskName interrupted: Attempting to kill Python Worker")
            PythonWorkerFactory.destroyPythonWorker(pythonExec, envVars, worker)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker", e)
          }
        }
      }
    }
  }

  override val arrowPythonRunner: ArrowPythonRunner = _arrowPythonRunner

  override def javaSideSocketServerRegister(): ServerSocket => Unit = {
    (server: ServerSocket) => {
      context.addTaskCompletionListener[Unit](_ => server.close())
    }
  }

  override def isTaskCompleteOrInterrupt(): () => Boolean = {
    () => {
      context.isCompleted || context.isInterrupted
    }
  }

  override def isTaskInterrupt(): () => Boolean = {
    () => {
      context.isInterrupted
    }
  }

  override def getTaskKillReason(): () => Option[String] = {
    () => {
      context.getKillReason
    }
  }

  override def killTaskIfInterrupted(): () => Unit = {
    () => {
      context.killTaskIfInterrupted
    }
  }

  override def readerRegister(callback: () => Unit): (ArrowStreamReader, BufferAllocator) => Unit = {
    (reader, allocator) => {
      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        try {
          allocator.close()
        } catch {
          case e: Exception =>
            logError("allocator.close()", e)
        }
      }
    }
  }
}


class JavaContext {
  val buffer = new ArrayBuffer[AppTaskCompletionListener]()

  var _isCompleted = false
  var _partitionId = UUID.randomUUID().toString
  var reasonIfKilled: Option[String] = None
  var getKillReason = reasonIfKilled

  def isInterrupted = reasonIfKilled.isDefined

  def isCompleted = _isCompleted

  def partitionId = _partitionId

  def markComplete = {
    _isCompleted = true
  }

  def markInterrupted(reason: String): Unit = {
    reasonIfKilled = Some(reason)
  }


  def killTaskIfInterrupted = {
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
  }


  def addTaskCompletionListener[U](f: (JavaContext) => U): JavaContext = {
    // Note that due to this scala bug: https://github.com/scala/bug/issues/11016, we need to make
    // this function polymorphic for every scala version >= 2.12, otherwise an overloaded method
    // resolution error occurs at compile time.
    _addTaskCompletionListener(new AppTaskCompletionListener {
      override def onTaskCompletion(context: JavaContext): Unit = f(context)
    })
  }

  def _addTaskCompletionListener(listener: AppTaskCompletionListener): JavaContext = {
    buffer += listener
    this
  }

  def close = {
    buffer.foreach(_.onTaskCompletion(this))
  }
}

trait AppTaskCompletionListener extends EventListener {
  def onTaskCompletion(context: JavaContext): Unit
}

class TaskKilledException(val reason: String) extends RuntimeException {
  def this() = this("unknown reason")
}
