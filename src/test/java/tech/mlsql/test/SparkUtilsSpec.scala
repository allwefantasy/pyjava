package tech.mlsql.test

import org.apache.spark.WowRowEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession, SparkUtils}
import org.scalatest.funsuite.AnyFunSuite

class SparkUtilsSpec extends AnyFunSuite {

  test("finds SparkSession internalCreateDataFrame overload used by SparkUtils") {
    val method = internalCreateDataFrameMethod(
      Class.forName("org.apache.spark.sql.classic.SparkSession"))

    assert(method.isDefined)
    assert(classOf[Dataset[_]].isAssignableFrom(method.get.getReturnType))
  }

  test("creates a DataFrame from catalyst rows when the Spark runtime can start") {
    assume(javaMajorVersion >= 17, "Spark 4.x runtime tests require Java 17 or newer")

    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkUtilsSpec")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val toCatalyst = WowRowEncoder.fromRow(schema)
      val rows = Seq(Row(1L, "alpha"), Row(2L, "beta"))
        .map(row => toCatalyst(row).copy())
      val rdd = spark.sparkContext.parallelize(rows, 1)
      assert(internalCreateDataFrameMethod(spark.getClass).isDefined)

      val df = SparkUtils.internalCreateDataFrame(spark, rdd, schema)

      assert(df.schema == schema)
      assert(df.collect().map(row => (row.getLong(0), row.getString(1))).toSeq == Seq(
        (1L, "alpha"),
        (2L, "beta")
      ))
    } finally {
      spark.stop()
    }
  }

  private def internalCreateDataFrameMethod(owner: Class[_]) = {
    owner.getMethods.find { method =>
      method.getName == "internalCreateDataFrame" &&
        method.getParameterCount == 3 &&
        method.getParameterTypes.apply(0).isAssignableFrom(classOf[RDD[_]]) &&
        method.getParameterTypes.apply(1).isAssignableFrom(classOf[StructType]) &&
        method.getParameterTypes.apply(2) == java.lang.Boolean.TYPE
    }
  }

  private def javaMajorVersion: Int = {
    val version = System.getProperty("java.specification.version")
    if (version.startsWith("1.")) {
      version.drop(2).toInt
    } else {
      version.takeWhile(_.isDigit).toInt
    }
  }
}
