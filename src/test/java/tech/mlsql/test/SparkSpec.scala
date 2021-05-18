package tech.mlsql.test

import java.util

import org.apache.spark.sql.{Row, SparkSession, SparkUtils}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.test.function.SparkFunctions.MockData

/**
 * 2019-08-14 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkSpec extends FunSuite with BeforeAndAfterAll with Logging{

  val rayEnv = new RayEnv
  var spark: SparkSession = null

  def condaEnv = "source /Users/allwefantasy/opt/anaconda3/bin/activate ray1.2"

  //spark.executor.heartbeatInterval
  test("test python ray connect") {
    val session = spark
    import session.implicits._
    val timezoneId = session.sessionState.conf.sessionLocalTimeZone

    val dataDF = session.createDataset(Range(0, 100).map(i => MockData(s"Title${i}", s"body-${i}"))).toDF()
    rayEnv.startDataServer(dataDF)

    val df = session.createDataset(rayEnv.dataServers).toDF()

    val envs = new util.HashMap[String, String]()
    envs.put("PYTHON_ENV", s"${condaEnv};export ARROW_PRE_0_15_IPC_FORMAT=1")
    //envs.put("PYTHONPATH", (os.pwd / "python").toString())

    val aps = new ApplyPythonScript(rayEnv.rayAddress, envs, timezoneId)
    val rayAddress = rayEnv.rayAddress
    logInfo(rayAddress)
    val func = aps.execute(
      s"""
         |import ray
         |import time
         |from pyjava.api.mlsql import RayContext
         |import numpy as np;
         |ray_context = RayContext.connect(globals(),"${rayAddress}")
         |def echo(row):
         |    row1 = {}
         |    row1["title"]=row['title'][1:]
         |    row1["body"]= row["body"] + ',' + row["body"]
         |    return row1
         |ray_context.foreach(echo)
          """.stripMargin, df.schema)

    val outputDF = df.rdd.mapPartitions(func)

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
    spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    super.beforeAll()
    rayEnv.startRay(condaEnv)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }
    rayEnv.stopRay(condaEnv)
    super.afterAll()
  }

}
