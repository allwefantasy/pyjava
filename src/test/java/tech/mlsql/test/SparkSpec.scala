package tech.mlsql.test

import java.util
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import tech.mlsql.arrow.python.ispark._
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction, SparkSocketRunner}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.test.RayEnv.ServerInfo
import tech.mlsql.test.function.SparkFunctions
import tech.mlsql.test.function.SparkFunctions.MockData

import java.util
import scala.collection.JavaConverters._

/**
 * 2019-08-14 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkSpec extends StreamTest {

  val rayEnv = new RayEnv

  //spark.executor.heartbeatInterval
  test("spark") {
    val session = spark
    import session.implicits._
    val timezoneid = session.sessionState.conf.sessionLocalTimeZone
    val df = session.createDataset[String](Seq("a1", "b1")).toDF("value")
    val struct = df.schema
    val abc = df.rdd.mapPartitions { iter =>
      val encoder = RowEncoder.apply(struct).resolveAndBind()
      val envs = new util.HashMap[String, String]()
      envs.put(str(PythonConf.PYTHON_ENV), "source activate dev && export ARROW_PRE_0_15_IPC_FORMAT=1")
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
        encoder.toRow(irow)
      }
      val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
      val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
      columnarBatchIter.flatMap { batch =>
        batch.rowIterator.asScala
      }.map(f => f.copy())
    }

    val wow = SparkUtils.internalCreateDataFrame(session, abc, StructType(Seq(StructField("AAA", LongType), StructField("BBB", LongType))), false)
    wow.show()
  }

  test("test python ray connect") {
    val session = spark
    import session.implicits._
    val timezoneId = session.sessionState.conf.sessionLocalTimeZone

    val dataDF = session.createDataset(Range(0, 100).map(i => MockData(s"Title${i}", s"body-${i}"))).toDF()
    rayEnv.startDataServer(dataDF)

    val df = session.createDataset(rayEnv.dataServers).toDF()
    val outputDF = df.rdd.mapPartitions(SparkFunctions.testScript1(df.schema, rayEnv.rayAddress, timezoneId))

    val pythonServers = SparkUtils.internalCreateDataFrame(session, outputDF, df.schema).collect()

    val rdd = session.sparkContext.makeRDD[Row](pythonServers, numSlices = pythonServers.length)
    val pythonOutputDF = rayEnv.collectResult(rdd)
    val output = SparkUtils.internalCreateDataFrame(session, pythonOutputDF, dataDF.schema).collect()
    assert(output.length == 100)
    output.zipWithIndex.foreach({
      case (row, index) =>
        assert(row.getString(0) == s"itle${index}")
        assert(row.getString(1) == s"body-${index},body-${index}")
    })
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    rayEnv.startRay()
  }

  override def afterAll(): Unit = {
    rayEnv.stopRay()
    super.afterAll()
  }

}
