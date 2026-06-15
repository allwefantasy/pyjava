package tech.mlsql.test

import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.ArrowConverters
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}

class ArrowConvertersSpec extends AnyFunSuite {

  test("serializes and reads Arrow batches without Spark task context") {
    val metadataSchema = StructType(Seq(
      StructField("active", BooleanType, nullable = false),
      StructField("note", StringType, nullable = true)
    ))
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("scores", ArrayType(IntegerType, containsNull = false), nullable = false),
      StructField("metadata", metadataSchema, nullable = false)
    ))

    val toCatalyst = WowRowEncoder.fromRow(schema)
    val toScala = WowRowEncoder.toRow(schema)
    val rows = Seq(
      Row(1L, "alpha", Seq(1, 2), Row(true, "first")),
      Row(2L, null, Seq.empty[Int], Row(false, null)),
      Row(3L, "gamma", Seq(5), Row(true, "last"))
    )
    val internalRows = rows.map(row => toCatalyst(row).copy()).iterator

    val writeContext = new AppContextImpl(new JavaContext, null)
    val batches = ArrowConverters
      .toBatchIterator(internalRows, schema, maxRecordsPerBatch = 2, timeZoneId = "UTC", writeContext)
      .toList

    assert(batches.length == 2)

    val readContext = new AppContextImpl(new JavaContext, null)
    val decoded = ArrowConverters
      .fromBatchIterator(batches.iterator, schema, timeZoneId = "UTC", readContext)
      .map(row => toScala(row.copy()))
      .toList

    assert(decoded.length == rows.length)
    assert(decoded.map(_.getLong(0)) == Seq(1L, 2L, 3L))
    assert(decoded.head.getString(1) == "alpha")
    assert(decoded.head.getSeq[Int](2).toSeq == Seq(1, 2))
    assert(decoded.head.getStruct(3).getBoolean(0))
    assert(decoded.head.getStruct(3).getString(1) == "first")
    assert(decoded(1).isNullAt(1))
    assert(decoded(1).getSeq[Int](2).isEmpty)
    assert(!decoded(1).getStruct(3).getBoolean(0))
    assert(decoded(1).getStruct(3).isNullAt(1))
    assert(decoded(2).getString(1) == "gamma")
  }
}
