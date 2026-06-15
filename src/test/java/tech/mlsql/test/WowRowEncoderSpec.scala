package tech.mlsql.test

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}

import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class WowRowEncoderSpec extends AnyFunSuite {

  test("round-trips rows with primitives, arrays, structs, and nulls") {
    val metadataSchema = StructType(Seq(
      StructField("active", BooleanType, nullable = false),
      StructField("note", StringType, nullable = true)
    ))
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false),
      StructField("scores", ArrayType(DoubleType, containsNull = false), nullable = false),
      StructField("metadata", metadataSchema, nullable = true),
      StructField("optional", StringType, nullable = true)
    ))

    val source = Row("alpha", 7, Seq(1.5, 2.25), Row(true, null), null)
    val toCatalyst = WowRowEncoder.fromRow(schema)
    val toScala = WowRowEncoder.toRow(schema)

    val internal = toCatalyst(source)
    assert(internal.isInstanceOf[InternalRow])
    assert(internal.numFields == schema.length)

    val decoded = toScala(internal)
    assert(decoded.getString(0) == "alpha")
    assert(decoded.getInt(1) == 7)
    assert(decoded.getSeq[Double](2).toSeq == Seq(1.5, 2.25))

    val metadata = decoded.getStruct(3)
    assert(metadata.getBoolean(0))
    assert(metadata.isNullAt(1))
    assert(decoded.isNullAt(4))
  }

  test("round-trips date, timestamp, decimal, and binary values") {
    val schema = StructType(Seq(
      StructField("event_date", DateType, nullable = false),
      StructField("event_time", TimestampType, nullable = false),
      StructField("amount", DecimalType(10, 2), nullable = false),
      StructField("payload", BinaryType, nullable = false)
    ))
    val eventDate = Date.valueOf("2026-06-15")
    val eventTime = Timestamp.valueOf("2026-06-15 12:34:56.123")
    val amount = new JBigDecimal("42.50")
    val payload = Array[Byte](1, 2, 3)
    val source = Row(eventDate, eventTime, amount, payload)

    val internal = WowRowEncoder.fromRow(schema)(source)
    val decoded = WowRowEncoder.toRow(schema)(internal)

    assert(decoded.getDate(0) == eventDate)
    assert(decoded.getTimestamp(1) == eventTime)
    assert(decoded.getDecimal(2).compareTo(amount) == 0)
    assert(decoded.getAs[Array[Byte]](3).toSeq == payload.toSeq)
  }

  test("handles empty schemas") {
    val schema = StructType(Nil)
    val internal = WowRowEncoder.fromRow(schema)(Row.empty)
    val decoded = WowRowEncoder.toRow(schema)(internal)

    assert(internal.numFields == 0)
    assert(decoded.length == 0)
  }
}
