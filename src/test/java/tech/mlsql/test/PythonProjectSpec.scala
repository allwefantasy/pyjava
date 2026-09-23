package tech.mlsql.test

import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite
import streaming.core.NotToRunTag
import java.io.File

import tech.mlsql.arrow.python.runner.PythonProjectRunner

/**
  * 2019-08-22 WilliamZhu(allwefantasy@gmail.com)
  */
@DoNotDiscover
class PythonProjectSpec extends AnyFunSuite with BeforeAndAfterAll {
  test("test python project", NotToRunTag) {
    val project = getExampleProject("pyproject1")
    val runner = new PythonProjectRunner(project, Map())
    val output = runner.run(Seq("bash", "-c", "source activate dev && python -u train.py"), Map(
      "tempDataLocalPath" -> "/tmp/data",
      "tempModelLocalPath" -> "/tmp/model"
    ))
    output.foreach(println)
  }

  def getExampleProject(name: String) = {
    new File(new File(getHome, "examples"), name).getPath
  }

  def getHome = {
    getClass.getResource("").getPath.split("target/test-classes").head
  }
}
