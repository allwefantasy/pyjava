package tech.mlsql.arrow.python.runner

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.{SparkException, TaskKilledException}
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.{ArrowBatchStreamWriter, ArrowConverters, ArrowUtils, Utils}
import tech.mlsql.common.utils.distribute.socket.server.SocketServerInExecutor
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._


class SparkSocketRunner(runnerName: String, host: String, timeZoneId: String) {

  def serveToStream(threadName: String)(writeFunc: OutputStream => Unit): Array[Any] = {
    val (_server, _host, _port) = SocketServerInExecutor.setupOneConnectionServer(host, runnerName)(s => {
      val out = new BufferedOutputStream(s.getOutputStream())
      Utils.tryWithSafeFinally {
        writeFunc(out)
      } {
        out.close()
      }
    })

    Array(_server, _host, _port)
  }

  def serveToStreamWithArrow(iter: Iterator[InternalRow], schema: StructType, maxRecordsPerBatch: Int, context: CommonTaskContext) = {
    serveToStream(runnerName) { out =>
      val batchWriter = new ArrowBatchStreamWriter(schema, out, timeZoneId)
      val arrowBatch = ArrowConverters.toBatchIterator(
        iter, schema, maxRecordsPerBatch, timeZoneId, context)
      batchWriter.writeBatches(arrowBatch)
      batchWriter.end()
    }
  }

  def readFromStreamWithArrow(host: String, port: Int, context: CommonTaskContext) = {
    val socket = new Socket(host, port)
    val stream = new DataInputStream(socket.getInputStream)
    val outfile = new DataOutputStream(socket.getOutputStream)
    new ReaderIterator[ColumnarBatch](stream, System.currentTimeMillis(), context) {
      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader ", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _
      context.readerRegister(() => {})(reader, allocator)

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>

                try {
                  reader = new ArrowStreamReader(stream, allocator)
                  root = reader.getVectorSchemaRoot()
                  schema = ArrowUtils.fromArrowSchema(root.getSchema())
                  vectors = root.getFieldVectors().asScala.map { vector =>
                    new ArrowColumnVector(vector)
                  }.toArray[ColumnVector]
                  read()
                } catch {
                  case e: IOException if (e.getMessage.contains("Missing schema") || e.getMessage.contains("Expected schema but header was")) =>
                    logInfo("Arrow read schema fail", e)
                    reader = null
                    read()
                }

              case SpecialLengths.ARROW_STREAM_CRASH =>
                read()

              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException(outfile)

              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection(outfile)
                null
            }
          }
        } catch handleException
      }
    }.flatMap { batch =>
      batch.rowIterator.asScala
    }
  }

}

object SparkSocketRunner {

}

abstract class ReaderIterator[OUT](
                                    stream: DataInputStream,
                                    startTime: Long,
                                    context: CommonTaskContext)
  extends Iterator[OUT] with Logging {

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


  protected def handlePythonException(out: DataOutputStream): SparkException = {
    // Signals that an exception has been thrown in python
    val exLength = stream.readInt()
    val obj = new Array[Byte](exLength)
    stream.readFully(obj)
    try {
      out.writeInt(SpecialLengths.END_OF_STREAM)
      out.flush()
    } catch {
      case e: Exception => logError("", e)
    }
    new SparkException(new String(obj, StandardCharsets.UTF_8), null)
  }

  protected def handleEndOfStream(out: DataOutputStream): Unit = {

    eos = true
  }

  protected def handleEndOfDataSection(out: DataOutputStream): Unit = {
    //read end of stream
    val flag = stream.readInt()
    if (flag != SpecialLengths.END_OF_STREAM) {
      logWarning(
        s"""
           |-----------------------WARNING--------------------------------------------------------------------
           |Here we should received message is SpecialLengths.END_OF_STREAM:${SpecialLengths.END_OF_STREAM}
           |But It's now ${flag}.
           |This may cause the **** python worker leak **** and make the ***interactive mode fails***.
           |--------------------------------------------------------------------------------------------------
           """.stripMargin)
    }
    try {
      out.writeInt(SpecialLengths.END_OF_STREAM)
      out.flush()
    } catch {
      case e: Exception => logError("", e)
    }

    eos = true
  }

  protected val handleException: PartialFunction[Throwable, OUT] = {
    case e: Exception if context.isTaskInterrupt()() =>
      logDebug("Exception thrown after task interruption", e)
      throw new TaskKilledException(context.getTaskKillReason()().getOrElse("unknown reason"))
    case e: SparkException =>
      throw e
    case eof: EOFException =>
      throw new SparkException("Python worker exited unexpectedly (crashed)", eof)

    case e: Exception =>
      throw new SparkException("Error to read", e)
  }
}
