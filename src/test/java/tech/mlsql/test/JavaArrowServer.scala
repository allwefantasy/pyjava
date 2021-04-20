package tech.mlsql.test

import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.network.NetUtils

/**
 * 24/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class JavaArrowServer extends FunSuite with BeforeAndAfterAll {

  test("test java arrow server") {
    val socketRunner = new SparkSocketRunner("wow", NetUtils.getHost, "Asia/Harbin")

    val dataSchema = StructType(Seq(StructField("value", StringType)))
    val encoder = WowRowEncoder.fromRow(dataSchema) //RowEncoder.apply(dataSchema).resolveAndBind()
    val newIter = Seq(Row.fromSeq(Seq("a1")), Row.fromSeq(Seq("a2"))).map { irow =>
      encoder(irow)
    }.iterator
    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, null)

    val Array(_, host, port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, 10, commonTaskContext)
    println(s"${host}:${port}")
    Thread.currentThread().join()
  }

  test("test read python arrow server") {
    val enconder = WowRowEncoder.toRow(StructType(Seq(StructField("a", LongType),StructField("b", LongType))))
    val socketRunner = new SparkSocketRunner("wow", NetUtils.getHost, "Asia/Harbin")
    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, null)
    val iter = socketRunner.readFromStreamWithArrow("127.0.0.1", 11111, commonTaskContext)
    iter.foreach(i => println(enconder(i.copy())))
    javaConext.close
  }

}
