package tech.mlsql.test

import tech.mlsql.arrow.python.runner.PythonProjectRunner
import tech.mlsql.common.utils.path.PathFun

/**
 * 4/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Main {
  def main(args: Array[String]): Unit = {
    val project = getExampleProject("pyproject1")
    val runner = new PythonProjectRunner(project, Map())
    val output = runner.run(Seq("bash", "-c", "source activate dev && python -u train.py"), Map(
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
