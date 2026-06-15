package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}

/**
  * 2019-08-13 WilliamZhu(allwefantasy@gmail.com)
  */
object SparkUtils {
  def internalCreateDataFrame(self: SparkSession,
                              catalystRows: RDD[InternalRow],
                              schema: StructType,
                              isStreaming: Boolean = false): DataFrame = {
    val method = self.getClass.getMethods.find { method =>
      method.getName == "internalCreateDataFrame" &&
        method.getParameterCount == 3 &&
        method.getParameterTypes.apply(0).isAssignableFrom(classOf[RDD[_]]) &&
        method.getParameterTypes.apply(1).isAssignableFrom(classOf[StructType])
    }.get
    method.invoke(self, catalystRows, schema, Boolean.box(isStreaming)).asInstanceOf[DataFrame]
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
