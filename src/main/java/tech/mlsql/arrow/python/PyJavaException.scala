package tech.mlsql.arrow.python

/**
  * 2019-08-15 WilliamZhu(allwefantasy@gmail.com)
  */
class PyJavaException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
