package tech.mlsql.arrow.python.runner

import java.io._
import java.net._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.{ArrowUtils, ArrowWriter, Utils}

import scala.collection.JavaConverters._


/**
  * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
  */
class ArrowPythonRunner(
                         funcs: Seq[ChainedPythonFunctions],
                         schema: StructType,
                         timeZoneId: String,
                         conf: Map[String, String])
  extends BasePythonRunner[Iterator[InternalRow], ColumnarBatch](
    funcs, conf) {

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  protected override def newWriterThread(
                                          worker: Socket,
                                          inputIterator: Iterator[Iterator[InternalRow]],
                                          partitionIndex: Int,
                                          context: CommonTaskContext): WriterThread = {
    new WriterThread(worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size + 1)
        for ((k, v) <- conf) {
          writeUTF(k, dataOut)
          writeUTF(v, dataOut)
        }
        writeUTF("timezone", dataOut)
        writeUTF(timeZoneId, dataOut)

        val command = funcs.head.funcs.head.command
        writeUTF(command, dataOut)

      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val arrowWriter = ArrowWriter.create(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          while (inputIterator.hasNext) {
            val nextBatch = inputIterator.next()

            while (nextBatch.hasNext) {
              arrowWriter.write(nextBatch.next())
            }

            arrowWriter.finish()
            writer.writeBatch()
            arrowWriter.reset()
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }

  protected override def newReaderIterator(
                                            stream: DataInputStream,
                                            writerThread: WriterThread,
                                            startTime: Long,
                                            worker: Socket,
                                            releasedOrClosed: AtomicBoolean,
                                            context: CommonTaskContext): Iterator[ColumnarBatch] = {
    new ReaderIterator(stream, writerThread, startTime, worker, releasedOrClosed, context) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _
      context.readerRegister(() => {})(reader, allocator)

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
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
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = ArrowUtils.fromArrowSchema(root.getSchema())
                vectors = root.getFieldVectors().asScala.map { vector =>
                  new ArrowColumnVector(vector)
                }.toArray[ColumnVector]
                read()

              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()

              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }
}
