package tech.mlsql.arrow.python.runner

import java.io._
import java.util.concurrent.atomic.AtomicReference

import tech.mlsql.arrow.Utils
import tech.mlsql.common.utils.log.Logging

import scala.io.Source

/**
  * 2019-08-22 WilliamZhu(allwefantasy@gmail.com)
  */
class PythonProjectRunner extends Logging {

  import PythonProjectRunner._

  def run(command: Seq[String],
          taskDirectory: String,
          env: Map[String, String],
          conf: Map[String, String]
         ) = {
    val proc = os.proc(command).spawn(
      cwd = os.Path(taskDirectory),
      env = env)
    val lines = Source.fromInputStream(proc.stdout)("utf-8").getLines
    val childThreadException = new AtomicReference[Throwable](null)
    // Start a thread to print the process's stderr to ours
    new Thread(s"stdin writer for $command") {
      def writeConf = {
        val dataOut = new DataOutputStream(proc.stdin)
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          Utils.writeUTF(k, dataOut)
          Utils.writeUTF(v, dataOut)
        }
      }

      override def run(): Unit = {
        try {
          writeConf
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          proc.stdin.close()
        }
      }
    }.start()

    // redirect err to other place(e.g. send them to driver)
    Utils.redirectStream(conf, proc.stderr)


    new Iterator[String] {
      def next(): String = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        val line = lines.next()
        line
      }

      def hasNext(): Boolean = {
        val result = if (lines.hasNext) {
          true
        } else {
          val exitStatus = try {
            proc.waitFor()
          }
          catch {
            case e: InterruptedException =>
              0
          }
          cleanup()
          if (exitStatus != 0) {
            val msg = s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" ")
            throw new IllegalStateException(msg)
          }
          false
        }
        propagateChildException
        result
      }

      private def cleanup(): Unit = {
        // cleanup task working directory if used
        scala.util.control.Exception.ignoring(classOf[IOException]) {
          if (conf.get(KEEP_LOCAL_DIR).map(_.toBoolean).getOrElse(false)) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
        }
        log.debug(s"Removed task working directory $taskDirectory")
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          proc.destroy()
          cleanup()
          throw t
        }
      }

    }
  }
}

object PythonProjectRunner {
  val KEEP_LOCAL_DIR = "keepLocalDir"
}
