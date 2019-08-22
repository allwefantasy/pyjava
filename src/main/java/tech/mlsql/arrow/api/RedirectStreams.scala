package tech.mlsql.arrow.api

import java.io.InputStream


/**
  * We will redirect all python daemon(or worker) stdout/stderr to the the java's stderr
  * by default. If you wanna change this behavior try to implements this Trait and
  * config the conf in ArrowPythonRunner.
  *
  * Example:
  *
  * new ArrowPythonRunner(
  * ....
  * conf=Map("python.redirect.impl"->"your impl class name")
  * )
  *
  */
trait RedirectStreams {

  def setConf(conf: Map[String, String]): Unit

  def conf: Map[String, String]

  def stdOut(stdOut: InputStream): Unit

  def stdErr(stdErr: InputStream): Unit
}
