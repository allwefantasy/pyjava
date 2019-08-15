package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}

/**
  * 2019-08-13 WilliamZhu(allwefantasy@gmail.com)
  */
object SparkUtils {
  def internalCreateDataFrame(self: SparkSession,
                              catalystRows: RDD[InternalRow],
                              schema: StructType,
                              isStreaming: Boolean = false): DataFrame = {
    val logicalPlan = LogicalRDD(
      schema.toAttributes,
      catalystRows,
      isStreaming = isStreaming)(self)
    Dataset.ofRows(self, logicalPlan)
  }

  def isFixDecimal(dt: DataType) = {
    dt match {
      case t@DecimalType.Fixed(precision, scale) => Option(new ArrowType.Decimal(precision, scale))
      case _ => None
    }
  }

  def setTaskContext(tc: TaskContext): Unit = TaskContext.setTaskContext(tc)

  def getKillReason(tc: TaskContext) = tc.getKillReason()

  def killTaskIfInterrupted(tc: TaskContext) = {
    tc.killTaskIfInterrupted()
  }
}
