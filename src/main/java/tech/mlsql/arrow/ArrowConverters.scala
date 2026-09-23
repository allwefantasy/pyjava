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

package tech.mlsql.arrow

import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkUtils}
import org.apache.spark.util.TaskCompletionListener
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, OutputStream}
import java.nio.channels.{Channels, ReadableByteChannel}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._


/**
 * Writes serialized ArrowRecordBatches to a DataOutputStream in the Arrow stream format.
 */
class ArrowBatchStreamWriter(
                              schema: StructType,
                              out: OutputStream,
                              timeZoneId: String) {

  val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
  val writeChannel = new WriteChannel(Channels.newChannel(out))

  // Write the Arrow schema first, before batches
  MessageSerializer.serialize(writeChannel, arrowSchema)

  /**
   * Consume iterator to write each serialized ArrowRecordBatch to the stream.
   */
  def writeBatches(arrowBatchIter: Iterator[Array[Byte]]): Unit = {
    arrowBatchIter.foreach(writeChannel.write)
  }

  /**
   * End the Arrow stream, does not close output stream.
   */
  def end(): Unit = {
    // 等后续无需兼容老版本arrow时，需要调整这行代码
    // writeChannel.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
    writeChannel.writeIntLittleEndian(0);
  }
}

object ArrowConverters {

  /**
   * Maps Iterator from InternalRow to serialized ArrowRecordBatches. Limit ArrowRecordBatch size
   * in a batch by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  def toBatchIterator(
                       rowIter: Iterator[InternalRow],
                       schema: StructType,
                       maxRecordsPerBatch: Int,
                       timeZoneId: String,
                       context: CommonTaskContext): Iterator[Array[Byte]] = {

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("toBatchIterator", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val unloader = new VectorUnloader(root)
    val arrowWriter = ArrowWriter.create(root)

    context match {
      case c: AppContextImpl => c.innerContext.asInstanceOf[JavaContext].addTaskCompletionListener { _ =>
        root.close()
        allocator.close()
      }
      case c: SparkContextImp => c.innerContext.asInstanceOf[TaskContext].addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          root.close()
          allocator.close()
        }
      })
    }

    new Iterator[Array[Byte]] {

      override def hasNext: Boolean = rowIter.hasNext || {
        root.close()
        allocator.close()
        false
      }

      override def next(): Array[Byte] = {
        val out = new ByteArrayOutputStream()
        val writeChannel = new WriteChannel(Channels.newChannel(out))

        Utils.tryWithSafeFinally {
          var rowCount = 0
          while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            val row = rowIter.next()
            arrowWriter.write(row)
            rowCount += 1
          }
          arrowWriter.finish()
          val batch = unloader.getRecordBatch()
          MessageSerializer.serialize(writeChannel, batch)
          batch.close()
        } {
          arrowWriter.reset()
        }

        out.toByteArray
      }
    }
  }

  /**
   * Writes a legacy Arrow stream (schema, record batches, end-of-stream int) straight to `out`.
   * Same bytes as [[ArrowBatchStreamWriter]] over [[toBatchIterator]], without a per-batch
   * `Array[Byte]` copy. Batches have both record and byte limits.
   * Does not close `out`. The writer thread owns the allocator; task cancellation should
   * close the socket so this call unblocks and releases it.
   */
  def writeLegacyArrowStream(
                              rowIter: Iterator[InternalRow],
                              schema: StructType,
                              maxRecordsPerBatch: Int,
                              timeZoneId: String,
                              out: OutputStream,
                              context: CommonTaskContext,
                              maxBytesPerBatch: Long = 8L * 1024 * 1024,
                              maxAllocation: Long = 128L * 1024 * 1024): Unit = {
    require(maxRecordsPerBatch > 0 && maxBytesPerBatch > 0 && maxAllocation >= maxBytesPerBatch)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      "arrow socket writer", 0, maxAllocation)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    val released = new AtomicBoolean(false)
    def release(): Unit = {
      if (released.compareAndSet(false, true)) {
        try root.close()
        finally allocator.close()
      }
    }
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    Utils.tryWithSafeFinally {
      MessageSerializer.serialize(writeChannel, arrowSchema)
      while (rowIter.hasNext) {
        Utils.tryWithSafeFinally {
          var rowCount = 0
          var batchBytes = 0L
          while (rowCount < maxRecordsPerBatch && batchBytes < maxBytesPerBatch && rowIter.hasNext) {
            context.killTaskIfInterrupted()()
            arrowWriter.write(rowIter.next())
            rowCount += 1
            batchBytes = root.getFieldVectors.asScala.map(_.getBufferSizeFor(rowCount).toLong).sum
            if (rowCount == 1 && batchBytes > maxBytesPerBatch) {
              throw new IllegalArgumentException("Single Arrow row exceeds python.arrow.maxBytesPerBatch")
            }
          }
          arrowWriter.finish()
          def serialize(part: VectorSchemaRoot): Unit = {
            val size = part.getFieldVectors.asScala.map(_.getBufferSize.toLong).sum
            if (size > maxBytesPerBatch && part.getRowCount > 1) {
              val half = part.getRowCount / 2
              val left = part.slice(0, half)
              try serialize(left) finally left.close()
              val right = part.slice(half, part.getRowCount - half)
              try serialize(right) finally right.close()
            } else {
              require(size <= maxBytesPerBatch, "Single Arrow row exceeds byte budget")
              val batch = new VectorUnloader(part).getRecordBatch()
              try MessageSerializer.serialize(writeChannel, batch) finally batch.close()
            }
          }
          serialize(root)
        } {
          arrowWriter.reset()
        }
      }
      // Legacy stream end. A continuation token would break ARROW_PRE_0_15_IPC_FORMAT readers.
      writeChannel.writeIntLittleEndian(0)
    } {
      release()
    }
  }

  /**
   * Maps iterator from serialized ArrowRecordBatches to InternalRows.
   */
  def fromBatchIterator(
                         arrowBatchIter: Iterator[Array[Byte]],
                         schema: StructType,
                         timeZoneId: String,
                         context: CommonTaskContext): Iterator[InternalRow] = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("fromBatchIterator", 0, Long.MaxValue)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    new Iterator[InternalRow] {
      private var rowIter = if (arrowBatchIter.hasNext) nextBatch() else Iterator.empty

      context match {
        case c: AppContextImpl => c.innerContext.asInstanceOf[JavaContext].addTaskCompletionListener { _ =>
          root.close()
          allocator.close()
        }
        case c: SparkContextImp => c.innerContext.asInstanceOf[TaskContext].addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            root.close()
            allocator.close()
          }
        })
      }

      override def hasNext: Boolean = rowIter.hasNext || {
        if (arrowBatchIter.hasNext) {
          rowIter = nextBatch()
          true
        } else {
          root.close()
          allocator.close()
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def nextBatch(): Iterator[InternalRow] = {
        val arrowRecordBatch = ArrowConverters.loadBatch(arrowBatchIter.next(), allocator)
        val vectorLoader = new VectorLoader(root)
        vectorLoader.load(arrowRecordBatch)
        arrowRecordBatch.close()

        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(columns)
        batch.setNumRows(root.getRowCount)
        batch.rowIterator().asScala
      }
    }
  }

  /**
   * Load a serialized ArrowRecordBatch.
   */
  def loadBatch(
                 batchBytes: Array[Byte],
                 allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayInputStream(batchBytes)
    MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(in)), allocator) // throws IOException
  }

  /**
   * Create a DataFrame from an RDD of serialized ArrowRecordBatches.
   */
  def toDataFrame(
                   arrowBatchRDD: JavaRDD[Array[Byte]],
                   schemaString: String,
                   sqlContext: SQLContext): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val timeZoneId = sqlContext.sparkSession.sessionState.conf.sessionLocalTimeZone
    val rdd = arrowBatchRDD.rdd.mapPartitions { iter =>
      val context = new SparkContextImp(TaskContext.get(), null)
      ArrowConverters.fromBatchIterator(iter, schema, timeZoneId, context)
    }
    SparkUtils.internalCreateDataFrame(sqlContext.sparkSession, rdd.setName("arrow"), schema)
  }

  /**
   * Read a file as an Arrow stream and parallelize as an RDD of serialized ArrowRecordBatches.
   */
  def readArrowStreamFromFile(
                               sqlContext: SQLContext,
                               filename: String): JavaRDD[Array[Byte]] = {
    Utils.tryWithResource(new FileInputStream(filename)) { fileStream =>
      // Create array to consume iterator so that we can safely close the file
      val batches = getBatchesFromStream(fileStream.getChannel).toArray
      // Parallelize the record batches to create an RDD
      JavaRDD.fromRDD(sqlContext.sparkContext.parallelize(batches, batches.length))
    }
  }

  /**
   * Read an Arrow stream input and return an iterator of serialized ArrowRecordBatches.
   */
  def getBatchesFromStream(in: ReadableByteChannel): Iterator[Array[Byte]] = {

    // Iterate over the serialized Arrow RecordBatch messages from a stream
    new Iterator[Array[Byte]] {
      var batch: Array[Byte] = readNextBatch()

      override def hasNext: Boolean = batch != null

      override def next(): Array[Byte] = {
        val prevBatch = batch
        batch = readNextBatch()
        prevBatch
      }

      // This gets the next serialized ArrowRecordBatch by reading message metadata to check if it
      // is a RecordBatch message and then returning the complete serialized message which consists
      // of a int32 length, serialized message metadata and a serialized RecordBatch message body
      def readNextBatch(): Array[Byte] = {
        val msgMetadata = MessageSerializer.readMessage(new ReadChannel(in))
        if (msgMetadata == null) {
          return null
        }

        // Get the length of the body, which has not been read at this point
        val bodyLength = msgMetadata.getMessageBodyLength.toInt

        // Only care about RecordBatch messages, skip Schema and unsupported Dictionary messages
        if (msgMetadata.getMessage.headerType() == MessageHeader.RecordBatch) {

          // Buffer backed output large enough to hold the complete serialized message
          val bbout = new ByteBufferOutputStream(4 + msgMetadata.getMessageLength + bodyLength)

          // Write message metadata to ByteBuffer output stream
          MessageSerializer.writeMessageBuffer(
            new WriteChannel(Channels.newChannel(bbout)),
            msgMetadata.getMessageLength,
            msgMetadata.getMessageBuffer)

          // Get a zero-copy ByteBuffer with already contains message metadata, must close first
          bbout.close()
          val bb = bbout.toByteBuffer
          bb.position(bbout.getCount())

          // Read message body directly into the ByteBuffer to avoid copy, return backed byte array
          bb.limit(bb.capacity())
          JavaUtils.readFully(in, bb)
          bb.array()
        } else {
          if (bodyLength > 0) {
            // Skip message body if not a RecordBatch
            Channels.newInputStream(in).skip(bodyLength)
          }

          // Proceed to next message
          readNextBatch()
        }
      }
    }
  }
}
