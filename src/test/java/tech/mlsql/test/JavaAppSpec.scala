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
    envs.put(str(PythonConf.PYTHON_ENV), "source activate streamingpro-spark-2.4.x")

    val dataSchema = StructType(Seq(StructField("value", StringType)))
    val enconder = RowEncoder.apply(dataSchema).resolveAndBind()
    val batch = new ArrowPythonRunner(
      Seq(ChainedPythonFunctions(Seq(PythonFunction(
        """
          |import pandas as pd
          |import numpy as np
          |for item in data_manager.fetch_once():
          |    print(item)
          |df = pd.DataFrame({'AAA': [4, 5, 6, 7],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
          |data_manager.set_output([[df['AAA'],df['BBB']]])
        """.stripMargin, envs, "python", "3.6")))), dataSchema,
      "GMT", Map()
    )
    val newIter = Seq(Row.fromSeq(Seq("a1")), Row.fromSeq(Seq("a2"))).map { irow =>
      enconder.toRow(irow)
    }.iterator
    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, batch)
    val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.foreach(f => println(f.copy()))
    javaConext.markComplete
    javaConext.close
  }
}
