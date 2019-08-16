package tech.mlsql.test

import java.util

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import tech.mlsql.arrow.python.ispark._
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str

import scala.collection.JavaConverters._

/**
  * 2019-08-14 WilliamZhu(allwefantasy@gmail.com)
  */
class SparkSpec extends StreamTest {
  //spark.executor.heartbeatInterval
  test("spark") {
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
            |for item in data_manager.fetch_once():
            |    print(item)
            |df = pd.DataFrame({'AAA': [4, 5, 6, 7],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
            |data_manager.set_output([[df['AAA'],df['BBB']]])
          """.stripMargin, envs, "python", "3.6")))), struct,
        timezoneid, Map()
      )
      val newIter = iter.map { irow =>
        enconder.toRow(irow)
      }
      val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
      val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
      columnarBatchIter.flatMap { batch =>
        batch.rowIterator.asScala
      }
    }
    //.count()
    val wow = SparkUtils.internalCreateDataFrame(session, abc, StructType(Seq(StructField("AAA", LongType), StructField("BBB", LongType))), false)
    wow.show()
    //    println(abc)
  }


}
