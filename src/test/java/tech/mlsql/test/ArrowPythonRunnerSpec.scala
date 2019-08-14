package tech.mlsql.test

import java.util

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.streaming.StreamTest
import tech.mlsql.arrow.python.ispark.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str

import scala.collection.JavaConverters._

/**
  * 2019-08-14 WilliamZhu(allwefantasy@gmail.com)
  */
class ArrowPythonRunnerSpec extends StreamTest {

  test("run") {
    val session = spark
    import session.implicits._
    val timezoneid = session.sessionState.conf.sessionLocalTimeZone
    val df = session.createDataset[String](Seq("a1", "b1")).toDF("value")
    val struct = df.schema
    val abc = df.rdd.mapPartitions { iter =>
      val enconder = RowEncoder.apply(struct).resolveAndBind()
      val envs = new util.HashMap[String, String]()
      envs.put(str(PythonConf.PYTHON_ENV), "source activate streamingpro-spark-2.4.x")
      val batch = new ArrowPythonRunner(
        Seq(ChainedPythonFunctions(Seq(PythonFunction(
          """
            |import pandas as pd
            |import numpy as np
            |for item in input_data.fetch_once():
            |    print(item)
            |t_dict = {'a' : 1, 'b': 2, 'c':3}
            |series_dict = pd.Series(t_dict)
            |input_data.output([series_dict],)
          """.stripMargin, envs, "python", "3.6")))), struct,
        timezoneid, Map()
      )
      val newIter = iter.map { irow =>
        enconder.toRow(irow)
      }
      val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), TaskContext.get())
      columnarBatchIter.flatMap { batch =>
        val a = batch.rowIterator.asScala
        while (a.hasNext) {
          val item = a.next().asInstanceOf[MutableColumnarRow]
          println(item.copy())
        }
        Seq().toIterator
      }
    }.count()
    println(abc)
  }
}
