package tech.mlsql.test

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.{TaskContext, WowRowEncoder}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str

import scala.collection.JavaConverters._

/**
 * 2019-08-15 WilliamZhu(allwefantasy@gmail.com)
 */
class JavaApp1Spec extends FunSuite
  with BeforeAndAfterAll {

  def condaEnv = "source /Users/allwefantasy/opt/anaconda3/bin/activate ray-dev"

  test("normal java application") {
    val envs = new util.HashMap[String, String]()
    envs.put(str(PythonConf.PYTHON_ENV), s"${condaEnv} && export ARROW_PRE_0_15_IPC_FORMAT=1 ")
    val sourceSchema = StructType(Seq(StructField("value", ArrayType(DoubleType))))

    val runnerConf = Map(
      "HOME" -> "",
      "OWNER" -> "",
      "GROUP_ID" -> "",
      "directData" -> "true",
      "runIn" -> "driver",
      "mode" -> "model",
      "rayAddress" -> "127.0.0.1:10001",
      "pythonMode" -> "ray"
    )

    val batch = new ArrowPythonRunner(
      Seq(ChainedPythonFunctions(Seq(PythonFunction(
        """
          |import ray
          |from pyjava.api.mlsql import RayContext
          |ray_context = RayContext.connect(globals(), context.conf["rayAddress"],namespace="default")
          |udfMaster = ray.get_actor("model_predict")
          |[index,worker] = ray.get(udfMaster.get.remote())
          |
          |input = [row["value"] for row in context.fetch_once_as_rows()]
          |try:
          |    res = ray.get(worker.apply.remote(input))
          |except Exception as inst:
          |    res=[]
          |    print(inst)
          |
          |udfMaster.give_back.remote(index)
          |ray_context.build_result([res])
        """.stripMargin, envs, "python", "3.6")))), sourceSchema,
      "GMT", runnerConf
    )


    val sourceEnconder = WowRowEncoder.fromRow(sourceSchema) //RowEncoder.apply(sourceSchema).resolveAndBind()
    val newIter = Seq(Row.fromSeq(Seq(Seq(1.1))), Row.fromSeq(Seq(Seq(1.2)))).map { irow =>
      sourceEnconder(irow).copy()
    }.iterator

    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, batch)
    val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
    val outputSchema = StructType(Seq(StructField("value", ArrayType(ArrayType(DoubleType)))))
    val outputEnconder = WowRowEncoder.toRow(outputSchema)
    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.map { r =>
      outputEnconder(r)
    }.toList.foreach(item => println(item.getAs[Seq[Seq[DoubleType]]](0)))

    javaConext.markComplete
    javaConext.close
  }

}
