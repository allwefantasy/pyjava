package tech.mlsql.test

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.arrow.python.runner.PythonProjectRunner
import tech.mlsql.common.utils.path.PathFun

/**
  * 2019-08-22 WilliamZhu(allwefantasy@gmail.com)
  */
class PythonProjectSpec extends FunSuite
  with BeforeAndAfterAll {
  test("test python project") {
    val project = getExampleProject("pyproject1")
    val runner = new PythonProjectRunner(project, Map())
    val output = runner.run(Seq("bash", "-c", "source activate streamingpro-spark-2.4.x && python train.py"), Map(
      "tempDataLocalPath" -> "/tmp/data",
      "tempModelLocalPath" -> "/tmp/model"
    ))
    output.foreach(println)
  }

  def getExampleProject(name: String) = {
    PathFun(getHome).add("examples").add(name).toPath
  }

  def getHome = {
    getClass.getResource("").getPath.split("target/test-classes").head
  }
}
