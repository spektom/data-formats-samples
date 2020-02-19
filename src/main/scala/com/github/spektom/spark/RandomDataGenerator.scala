package com.github.spektom.spark

import java.math.MathContext
import java.nio.ByteBuffer

import com.github.spektom.spark.RandomDataGenerator._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.CalendarInterval

import scala.collection.mutable
import scala.util.Random

object RandomDataGenerator {

  // Various decimal types configurations
  val BooleanDecimal: DecimalType = DecimalType(1, 0)
  val ByteDecimal: DecimalType = DecimalType(3, 0)
  val ShortDecimal: DecimalType = DecimalType(5, 0)
  val IntDecimal: DecimalType = DecimalType(10, 0)
  val LongDecimal: DecimalType = DecimalType(20, 0)
  val FloatDecimal: DecimalType = DecimalType(14, 7)
  val DoubleDecimal: DecimalType = DecimalType(30, 15)
  val BigIntDecimal: DecimalType = DecimalType(38, 0)

  val LOREM_IPSUM: Array[String] = "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium totam rem aperiam eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt neque porro quisquam est qui dolorem ipsum quia dolor sit amet consectetur adipisci velit sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem Ut enim ad minima veniam quis nostrum exercitationem ullam corporis suscipit laboriosam nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur vel illum qui dolorem eum fugiat quo voluptas nulla pariatur At vero eos et accusamus et iusto odio dignissimos ducimus qui blanditiis praesentium voluptatum deleniti atque corrupti quos dolores et quas molestias excepturi sint obcaecati cupiditate non provident similique sunt in culpa qui officia deserunt mollitia animi id est laborum et dolorum fuga Et harum quidem rerum facilis est et expedita distinctio Nam libero tempore cum soluta nobis est eligendi optio cumque nihil impedit quo minus id quod maxime placeat facere possimus omnis voluptas assumenda est omnis dolor repellendus Temporibus autem quibusdam et aut officiis debitis aut rerum necessitatibus saepe eveniet ut et voluptates repudiandae sint et molestiae non recusandae Itaque earum rerum hic tenetur a sapiente delectus ut aut reiciendis voluptatibus maiores alias consequatur aut perferendis doloribus asperiores repellat".split(" ")

  case object UUIDStringType extends DataType {
    override def defaultSize: Int = 16

    override def asNullable: DataType = this
  }

  case object UUIDBytesType extends DataType {
    override def defaultSize: Int = 16

    override def asNullable: DataType = this
  }

}

/**
 * Random data generators for Spark SQL DataTypes. These generators do not generate uniformly random
 * values; instead, they're biased to return "interesting" values (such as maximum / minimum values)
 * with higher probability.
 *
 * @param config Configuration
 * @param random Optional random generator instance
 */
class RandomDataGenerator(config: Config,
                          random: Random = new Random(System.nanoTime())) {

  private object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  /**
   * Helper function for constructing a biased random number generator which returns "interesting"
   * values with a higher probability.
   */
  private def randomNumeric[T](uniformRand: Random => T,
                               interestingValues: Seq[T]): Some[() => T] = {
    val f = () => {
      if (random.nextFloat() <= config.probabilityOfInteresting) {
        interestingValues(random.nextInt(interestingValues.length))
      } else {
        uniformRand(random)
      }
    }
    Some(f)
  }

  private def randomText(): String = {
    if (random.nextFloat() <= config.probabilityOfInteresting) {
      random.nextString(random.nextInt(config.maxTextLength))
    } else {
      var s = ""
      var n = random.nextInt(10)
      while (n > 0) {
        if (s.length > 0) {
          s += " "
        }
        s += LOREM_IPSUM(random.nextInt(LOREM_IPSUM.length))
        n -= 1
      }
      if (s.length > config.maxTextLength) s.substring(0, config.maxTextLength) else s
    }
  }

  private def randomUUID(): java.util.UUID = {
    if (random.nextFloat() <= config.probabilityOfInteresting) {
      new java.util.UUID(0L, 0L)
    } else {
      java.util.UUID.randomUUID()
    }
  }

  /**
   * A wrapper of Float.intBitsToFloat to use a unique NaN value for all NaN values.
   * This prevents `checkEvaluationWithUnsafeProjection` from failing due to
   * the difference between `UnsafeRow` binary presentation for NaN.
   * This is visible for testing.
   */
  def intBitsToFloat(bits: Int): Float = {
    val value = java.lang.Float.intBitsToFloat(bits)
    if (value.isNaN) Float.NaN else value
  }

  /**
   * A wrapper of Double.longBitsToDouble to use a unique NaN value for all NaN values.
   * This prevents `checkEvaluationWithUnsafeProjection` from failing due to
   * the difference between `UnsafeRow` binary presentation for NaN.
   * This is visible for testing.
   */
  def longBitsToDouble(bits: Long): Double = {
    val value = java.lang.Double.longBitsToDouble(bits)
    if (value.isNaN) Double.NaN else value
  }

  /**
   * Returns column name according to data type.
   *
   * @param dataType the type to generate column name for
   * @param idx      Column index
   */
  def columnName(dataType: DataType, idx: Int): String = {
    val prefix = dataType match {
      case ArrayType(_, _) => "arr"
      case BinaryType => "bin"
      case BooleanType => "bool"
      case ByteType => "byte"
      case DateType => "date"
      case DoubleType => "real"
      case Fixed(_, _) => "dec"
      case FloatType => "float"
      case IntegerType => "int"
      case LongType => "long"
      case MapType(_, _, _) => "map"
      case NullType => "null"
      case ShortType => "short"
      case StringType => "str"
      case StructType(_) => "obj"
      case TimestampType => "ts"
      case UUIDStringType => "uuids"
      case UUIDBytesType => "uuidb"
      case _ => throw new IllegalStateException(s"Unsupported type: $dataType")
    }
    prefix + "_" + BigInt.apply(idx).toString(36)
  }

  /**
   * Returns random type from the list of data types.
   * All different decimal types (DecimalType) or UUID types have the same probability.
   */
  def randomType(acceptedTypes: Seq[DataType]): DataType = {
    val otherTypes = acceptedTypes.filterNot(dt => dt.isInstanceOf[DecimalType] || dt.getClass.getName.contains("UUID"))
    val decimalTypes = acceptedTypes.filter(dt => dt.isInstanceOf[DecimalType])
    val uuidTypes = acceptedTypes.filter(dt => dt.getClass.getName.contains("UUID"))
    val n = random.nextInt(otherTypes.size + 2)
    if (n == otherTypes.size + 1) {
      decimalTypes(random.nextInt(decimalTypes.size))
    } else if (n == otherTypes.size) {
      uuidTypes(random.nextInt(uuidTypes.size))
    } else {
      otherTypes(n)
    }
  }

  def toNativeType(dt: DataType): DataType = {
    dt match {
      case UUIDBytesType => BinaryType
      case UUIDStringType => StringType
      case x => x
    }
  }

  def structField(col: String, dt: DataType): StructField = {
    StructField(col, toNativeType(dt), random.nextBoolean())
  }

  /**
   * Returns a randomly generated schema, based on the given accepted types.
   *
   * @param numFields     the number of fields in this schema
   * @param acceptedTypes aypes to draw from.
   */
  def randomSchema(numFields: Int, acceptedTypes: Seq[DataType]): StructType = {
    StructType(Seq.tabulate(numFields) { i =>
      val dt = randomType(acceptedTypes)
      structField(columnName(dt, i), dt)
    })
  }

  /**
   * Returns a random nested schema. This will randomly generate structs and arrays drawn from
   * acceptedTypes.
   */
  def randomNestedSchema(totalFields: Int, acceptedTypes: Seq[DataType]): StructType = {
    val fields = mutable.ArrayBuffer.empty[StructField]
    var i = 0
    var numFields = totalFields
    while (numFields > 0) {
      val v = random.nextInt(acceptedTypes.size + 3)
      if (v < acceptedTypes.size) {
        // Simple type:
        val dt = randomType(acceptedTypes)
        fields += structField(columnName(dt, i), dt)
        numFields -= 1
      } else if (v == acceptedTypes.size) {
        // Map
        if (random.nextFloat() < config.probabilityOfNestedMap) {
          // Map with nested value
          val n = Math.max(random.nextInt(numFields), 1)
          val nested = randomNestedSchema(n, acceptedTypes)
          val mapType = MapType(StringType, nested, random.nextBoolean())
          fields += structField(columnName(mapType, i), mapType)
          numFields -= n
        } else {
          // Map with primitive value
          val dt = randomType(acceptedTypes)
          val mapType = MapType(StringType, toNativeType(dt), random.nextBoolean())
          fields += structField(columnName(mapType, i), mapType)
          numFields -= 1
        }
      } else if (v == acceptedTypes.size + 1) {
        // Array
        if (random.nextFloat() < config.probabilityOfNestedArray) {
          // Array with nested value
          val n = Math.max(random.nextInt(numFields), 1)
          val nested = randomNestedSchema(n, acceptedTypes)
          val arrayType = ArrayType(nested)
          fields += structField(columnName(arrayType, i), arrayType)
          numFields -= n
        } else {
          // Array with primitive value
          val dt = randomType(acceptedTypes)
          val arrayType = ArrayType(toNativeType(dt))
          fields += structField(columnName(arrayType, i), arrayType)
          numFields -= 1
        }
      } else {
        // Struct
        val n = Math.max(random.nextInt(numFields), 1)
        val nested = randomNestedSchema(n, acceptedTypes)
        fields += structField(columnName(nested, i), nested)
        numFields -= n
      }
      i += 1
    }
    StructType(fields)
  }

  /**
   * Returns a function which generates random values for the given `DataType`, or `None` if no
   * random data generator is defined for that data type. The generated values will use an external
   * representation of the data type; for example, the random generator for `DateType` will return
   * instances of [[java.sql.Date]] and the generator for `StructType` will return a [[Row]].
   * For a `UserDefinedType` for a class X, an instance of class X is returned.
   *
   * @param dataType the type to generate values for
   * @param nullable whether null values should be generated
   * @return a function which can be called to generate random values.
   */
  def forType(dataType: DataType,
              nullable: Boolean = true): Option[() => Any] = {
    val valueGenerator: Option[() => Any] = dataType match {
      case StringType => Some(() => randomText())
      case BinaryType => Some(() => {
        val arr = new Array[Byte](random.nextInt(config.maxTextLength))
        random.nextBytes(arr)
        arr
      })
      case UUIDStringType => Some(() => randomUUID().toString)
      case UUIDBytesType => Some(() => {
        val uuid = randomUUID()
        ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()
      })
      case BooleanType => Some(() => random.nextBoolean())
      case DateType =>
        val generator =
          () => {
            var milliseconds = random.nextLong() % 253402329599999L
            // -62135740800000L is the number of milliseconds before January 1, 1970, 00:00:00 GMT
            // for "0001-01-01 00:00:00.000000". We need to find a
            // number that is greater or equals to this number as a valid timestamp value.
            while (milliseconds < -62135740800000L) {
              // 253402329599999L is the number of milliseconds since
              // January 1, 1970, 00:00:00 GMT for "9999-12-31 23:59:59.999999".
              milliseconds = random.nextLong() % 253402329599999L
            }
            DateTimeUtils.toJavaDate((milliseconds / DateTimeUtils.MILLIS_PER_DAY).toInt)
          }
        Some(generator)
      case TimestampType =>
        val generator =
          () => {
            var milliseconds = random.nextLong() % 253402329599999L
            // -62135740800000L is the number of milliseconds before January 1, 1970, 00:00:00 GMT
            // for "0001-01-01 00:00:00.000000". We need to find a
            // number that is greater or equals to this number as a valid timestamp value.
            while (milliseconds < -62135740800000L) {
              // 253402329599999L is the number of milliseconds since
              // January 1, 1970, 00:00:00 GMT for "9999-12-31 23:59:59.999999".
              milliseconds = random.nextLong() % 253402329599999L
            }
            // DateTimeUtils.toJavaTimestamp takes microsecond.
            DateTimeUtils.toJavaTimestamp(milliseconds * 1000)
          }
        Some(generator)
      case CalendarIntervalType => Some(() => {
        val months = random.nextInt(1000)
        val ns = random.nextLong()
        new CalendarInterval(months, ns)
      })
      case Fixed(precision, scale) => Some(
        () => BigDecimal.apply(
          random.nextLong() % math.pow(10, precision).toLong,
          scale,
          new MathContext(precision)).bigDecimal)
      case DoubleType => randomNumeric[Double](
        r => longBitsToDouble(r.nextLong()), Seq(Double.MinValue, Double.MinPositiveValue,
          Double.MaxValue, Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, 0.0))
      case FloatType => randomNumeric[Float](
        r => intBitsToFloat(r.nextInt()), Seq(Float.MinValue, Float.MinPositiveValue,
          Float.MaxValue, Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, 0.0f))
      case ByteType => randomNumeric[Byte](
        _.nextInt().toByte, Seq(Byte.MinValue, Byte.MaxValue, 0.toByte))
      case IntegerType => randomNumeric[Int](
        _.nextInt(), Seq(Int.MinValue, Int.MaxValue, 0))
      case LongType => randomNumeric[Long](
        _.nextLong(), Seq(Long.MinValue, Long.MaxValue, 0L))
      case ShortType => randomNumeric[Short](
        _.nextInt().toShort, Seq(Short.MinValue, Short.MaxValue, 0.toShort))
      case NullType => Some(() => null)
      case ArrayType(elementType, containsNull) =>
        forType(elementType, nullable = containsNull).map {
          val maxArraySize = elementType match {
            case _: StructType => config.maxNestedArraySize
            case _ => config.maxPrimitiveArraySize
          }
          elementGenerator => () => Seq.fill(random.nextInt(maxArraySize))(elementGenerator())
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        for (
          keyGenerator <- forType(keyType, nullable = false);
          valueGenerator <-
            forType(valueType, nullable = valueContainsNull)
        ) yield {
          () => {
            val maxMapSize = valueType match {
              case _: StructType => config.maxNestedMapSize
              case _ => config.maxPrimitiveMapSize
            }
            val length = random.nextInt(maxMapSize)
            val keys = scala.collection.mutable.HashSet(Seq.fill(length)(keyGenerator()): _*)
            // In case the number of different keys is not enough, set a max iteration to avoid
            // infinite loop.
            var count = 0
            while (keys.size < length && count < maxMapSize) {
              keys += keyGenerator()
              count += 1
            }
            val values = Seq.fill(keys.size)(valueGenerator())
            keys.zip(values).toMap
          }
        }
      case StructType(fields) =>
        val maybeFieldGenerators: Seq[Option[() => Any]] = fields.map { field =>
          forType(field.dataType, nullable = field.nullable)
        }
        if (maybeFieldGenerators.forall(_.isDefined)) {
          val fieldGenerators: Seq[() => Any] = maybeFieldGenerators.map(_.get)
          Some(() => Row.fromSeq(fieldGenerators.map(_.apply())))
        } else {
          None
        }
      case _ => None
    }
    // Handle nullability by wrapping the non-null value generator:
    valueGenerator.map { valueGenerator =>
      if (nullable) {
        () => {
          if (random.nextFloat() <= config.probabilityOfNull) {
            null
          } else {
            valueGenerator()
          }
        }
      } else {
        valueGenerator
      }
    }
  }

  /**
   * Generates a random row for `schema`.
   */
  def randomRow(schema: StructType): Row = {
    val fields = mutable.ArrayBuffer.empty[Any]
    schema.fields.foreach { f =>
      f.dataType match {
        case StructType(children) =>
          fields += randomRow(StructType(children))
        case _ =>
          val generator =
            if (f.name.startsWith("uuids"))
              forType(UUIDStringType, f.nullable)
            else if (f.name.startsWith("uuidb"))
              forType(UUIDBytesType, f.nullable)
            else
              forType(f.dataType, f.nullable)
          assert(generator.isDefined, "Unsupported type")
          val gen = generator.get
          fields += gen()
      }
    }
    Row.fromSeq(fields)
  }

  /**
   * Generates random data frame.
   *
   * @param spark         Spark session
   * @param flatSchema    Whether the output schema is flat or hierarchical
   * @param acceptedTypes Various types to use in generated schema
   */
  def randomDataset(spark: SparkSession,
                    flatSchema: Boolean,
                    acceptedTypes: Array[DataType] = Array(
                      BinaryType, BooleanType, ByteType, DateType, FloatType, DoubleType, IntegerType, LongType,
                      ShortType, StringType, TimestampType, UUIDStringType, UUIDBytesType,
                      BooleanDecimal, ShortDecimal, IntDecimal, ByteDecimal, FloatDecimal, LongDecimal,
                      DoubleDecimal, BigIntDecimal, new DecimalType(5, 2),
                      new DecimalType(12, 2), new DecimalType(30, 10))
                   ): DataFrame = {

    val schema = if (flatSchema) {
      randomSchema(config.flatSchemaFields, acceptedTypes)
    } else {
      randomNestedSchema(config.hierSchemaFields, acceptedTypes)
    }
    val rows = (1 to config.rowsNumber).map(_ => randomRow(schema))
    val rdd = spark.sparkContext.makeRDD(rows)
    spark.createDataFrame(rdd, schema)
  }
}
