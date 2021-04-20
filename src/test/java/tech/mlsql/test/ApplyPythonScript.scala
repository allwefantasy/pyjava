package tech.mlsql.test

import org.apache.spark.{TaskContext, WowRowEncoder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonFunction}

import scala.collection.JavaConverters._

/**
 * 19/4/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplyPythonScript(_rayAddress: String, _envs: java.util.HashMap[String, String], _timezoneId: String, _pythonMode: String = "ray") {
  def execute(script: String, struct: StructType): Iterator[Row] => Iterator[InternalRow] = {
    //    val rayAddress = _rayAddress
    val envs = _envs
    val timezoneId = _timezoneId
    val pythonMode = _pythonMode

    iter =>
      val encoder = WowRowEncoder.fromRow(struct)
        //RowEncoder.apply(struct).resolveAndBind()
      val batch = new ArrowPythonRunner(
        Seq(ChainedPythonFunctions(Seq(PythonFunction(
          script, envs, "python", "3.6")))), struct,
        timezoneId, Map("pythonMode" -> pythonMode)
      )
      val newIter = iter.map(encoder)
      val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
      val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
      columnarBatchIter.flatMap(_.rowIterator.asScala).map(f => f.copy())
  }
}
