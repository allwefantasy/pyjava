package tech.mlsql.test

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.arrow.ArrowUtils

import scala.collection.JavaConverters._

class ArrowUtilsSpec extends AnyFunSuite {

  test("maps scalar Spark SQL types to Arrow and back") {
    val scalarTypes = Seq[DataType](
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      DecimalType(12, 4)
    )

    scalarTypes.foreach { dataType =>
      assert(ArrowUtils.fromArrowType(ArrowUtils.toArrowType(dataType, "UTC")) == dataType)
    }
  }

  test("round-trips nested schemas and preserves nullability") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("tags", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("profile", StructType(Seq(
        StructField("score", DoubleType, nullable = false),
        StructField("amount", DecimalType(12, 2), nullable = true)
      )), nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("event_date", DateType, nullable = true)
    ))

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "Asia/Shanghai")
    val createdAtType = arrowSchema.getFields.asScala
      .find(_.getName == "created_at")
      .get
      .getType
      .asInstanceOf[ArrowType.Timestamp]

    assert(createdAtType.getUnit == TimeUnit.MICROSECOND)
    assert(createdAtType.getTimezone == "Asia/Shanghai")
    assert(ArrowUtils.fromArrowSchema(arrowSchema) == schema)
  }

  test("uses the expected Arrow encodings for date and floating point values") {
    assert(ArrowUtils.toArrowType(DateType, "UTC").asInstanceOf[ArrowType.Date].getUnit == DateUnit.DAY)
    assert(ArrowUtils.toArrowType(FloatType, "UTC").asInstanceOf[ArrowType.FloatingPoint].getPrecision == FloatingPointPrecision.SINGLE)
    assert(ArrowUtils.toArrowType(DoubleType, "UTC").asInstanceOf[ArrowType.FloatingPoint].getPrecision == FloatingPointPrecision.DOUBLE)
  }

  test("requires a time zone when converting TimestampType") {
    val error = intercept[UnsupportedOperationException] {
      ArrowUtils.toArrowField("created_at", TimestampType, nullable = true, timeZoneId = null)
    }

    assert(error.getMessage.contains("timestamp must supply timeZoneId parameter"))
  }
}
