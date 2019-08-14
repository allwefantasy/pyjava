package tech.mlsql.arrow

import java.io.{Closeable, IOException, InputStream, OutputStream}
import java.util.concurrent.TimeUnit

import org.apache.spark.util.Utils.logError
import tech.mlsql.common.utils.log.Logging

import scala.io.Source
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}


/**
  * 2019-08-13 WilliamZhu(allwefantasy@gmail.com)
  */
object Utils extends Logging {
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }


  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }


  /**
    * Return the stderr of a process after waiting for the process to terminate.
    * If the process does not terminate within the specified timeout, return None.
    */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)
    if (terminated) {
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    } else {
      None
    }
  }

  /**
    * Execute the given block, logging and re-throwing any uncaught exception.
    * This is particularly useful for wrapping code that runs in a thread, to ensure
    * that exceptions are printed, and to avoid having to catch Throwable.
    */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }


  class RedirectThread(
                        in: InputStream,
                        out: OutputStream,
                        name: String,
                        propagateEof: Boolean = false)
    extends Thread(name) {

    setDaemon(true)

    override def run() {
      scala.util.control.Exception.ignoring(classOf[IOException]) {
        // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
        Utils.tryWithSafeFinally {
          val buf = new Array[Byte](1024)
          var len = in.read(buf)
          while (len != -1) {
            out.write(buf, 0, len)
            out.flush()
            len = in.read(buf)
          }
        } {
          if (propagateEof) {
            out.close()
          }
        }
      }
    }
  }

  /** Executes the given block in a Try, logging any uncaught exceptions. */
  def tryLog[T](f: => T): Try[T] = {
    try {
      val res = f
      scala.util.Success(res)
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        scala.util.Failure(t)
    }
  }

  /** Returns true if the given exception was fatal. See docs for scala.util.control.NonFatal. */
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) |
           _: InterruptedException |
           _: NotImplementedError |
           _: ControlThrowable |
           _: LinkageError =>
        false
      case _ =>
        true
    }
  }

}
