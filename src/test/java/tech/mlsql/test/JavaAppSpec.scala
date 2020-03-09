package tech.mlsql.test

import java.util

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str

import scala.collection.JavaConverters._

/**
 * 2019-08-15 WilliamZhu(allwefantasy@gmail.com)
 */
class JavaAppSpec extends FunSuite
  with BeforeAndAfterAll {
  test("normal java application") {
    val envs = new util.HashMap[String, String]()
    envs.put(str(PythonConf.PYTHON_ENV), "source activate dev && export ARROW_PRE_0_15_IPC_FORMAT=1 ")
    val sourceSchema = StructType(Seq(StructField("value", StringType)))
    val batch = new ArrowPythonRunner(
      Seq(ChainedPythonFunctions(Seq(PythonFunction(
        """
          |import pandas as pd
          |import numpy as np
          |
          |def process():
          |    for item in context.fetch_once_as_rows():
          |        item["value1"] = item["value"] + "_suffix"
          |        yield item
          |
          |context.build_result(process())
        """.stripMargin, envs, "python", "3.6")))), sourceSchema,
      "GMT", Map()
    )


    val sourceEnconder = RowEncoder.apply(sourceSchema).resolveAndBind()
    val newIter = Seq(Row.fromSeq(Seq("a1")), Row.fromSeq(Seq("a2"))).map { irow =>
      sourceEnconder.toRow(irow).copy()
    }.iterator

    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, batch)
    val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
    //copy is required 
    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.foreach(f => println(f.copy()))
    javaConext.markComplete
    javaConext.close
  }
}
