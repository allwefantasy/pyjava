package tech.mlsql.arrow.python.ispark

import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkUtils
import org.apache.spark.util.TaskCompletionListener
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.runner.ArrowPythonRunner
import tech.mlsql.common.utils.log.Logging

/**
 * 2019-08-15 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkContextImp(context: TaskContext, _arrowPythonRunner: ArrowPythonRunner) extends CommonTaskContext with Logging {
  override def pythonWorkerRegister(callback: () => Unit) = {
    (releasedOrClosed: AtomicBoolean,
     reuseWorker: Boolean,
     worker: Socket
    ) => {
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
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
      })
    }
  }

  override def assertTaskIsCompleted(callback: () => Unit) = {
    () => {
      assert(context.isCompleted)
    }
  }

  override def setTaskContext(): () => Unit = {
    () => {
      SparkUtils.setTaskContext(context)
    }
  }

  override def innerContext: Any = context

  override def isBarrier: Boolean = context.getClass.getName == "org.apache.spark.BarrierTaskContext"

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
            val taskName = s"${context.partitionId}.${context.attemptNumber} " +
              s"in stage ${context.stageId} (TID ${context.taskAttemptId})"
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
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          server.close()
        }
      })
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
      SparkUtils.getKillReason(context)
    }
  }

  override def killTaskIfInterrupted(): () => Unit = {
    () => {
      SparkUtils.killTaskIfInterrupted(context)
    }
  }

  override def readerRegister(callback: () => Unit): (ArrowStreamReader, BufferAllocator) => Unit = {
    (reader, allocator) => {
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          if (reader != null) {
            reader.close(false)
          }
          // 这里有个特殊情况，用户可能只会读取部分数据，这个时候进行close，会
          // 显示内存泄露，此时进行close会抛错，我们需要catch住这个错误。
          // 目前来看，资源应该能够得到释放。大部分情况，我们都能正常消费掉所有数据。
          try {
            allocator.close()
          } catch {
            case e: Exception =>
              logError("allocator.close()", e)
          }

        }
      })
    }
  }
}
