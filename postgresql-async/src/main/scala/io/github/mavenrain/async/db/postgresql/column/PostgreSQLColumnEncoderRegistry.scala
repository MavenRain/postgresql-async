/*
 * Copyright 2021 Onyekachukwu Obi
 *
 * Onyekachukwu Obi licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.mavenrain.async.db.postgresql.column

import io.github.mavenrain.async.db.column._
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import org.joda.time._
import scala.jdk.CollectionConverters._

object PostgreSQLColumnEncoderRegistry {
  val Instance = new PostgreSQLColumnEncoderRegistry()
}

class PostgreSQLColumnEncoderRegistry extends ColumnEncoderRegistry {

  private val classesSequence_ : List[(Class[_], (ColumnEncoder, Int))] = List(
    classOf[Int] -> (IntegerEncoderDecoder -> ColumnTypes.Numeric),
    classOf[java.lang.Integer] -> (IntegerEncoderDecoder -> ColumnTypes.Numeric),

    classOf[java.lang.Short] -> (ShortEncoderDecoder -> ColumnTypes.Numeric),
    classOf[Short] -> (ShortEncoderDecoder -> ColumnTypes.Numeric),

    classOf[Long] -> (LongEncoderDecoder -> ColumnTypes.Numeric),
    classOf[java.lang.Long] -> (LongEncoderDecoder -> ColumnTypes.Numeric),

    classOf[String] -> (StringEncoderDecoder -> ColumnTypes.Varchar),
    classOf[java.lang.String] -> (StringEncoderDecoder -> ColumnTypes.Varchar),

    classOf[Float] -> (FloatEncoderDecoder -> ColumnTypes.Numeric),
    classOf[java.lang.Float] -> (FloatEncoderDecoder -> ColumnTypes.Numeric),

    classOf[Double] -> (DoubleEncoderDecoder -> ColumnTypes.Numeric),
    classOf[java.lang.Double] -> (DoubleEncoderDecoder -> ColumnTypes.Numeric),

    classOf[BigDecimal] -> (BigDecimalEncoderDecoder -> ColumnTypes.Numeric),
    classOf[java.math.BigDecimal] -> (BigDecimalEncoderDecoder -> ColumnTypes.Numeric),

    classOf[java.net.InetAddress] -> (InetAddressEncoderDecoder -> ColumnTypes.Inet),

    classOf[java.util.UUID] -> (UUIDEncoderDecoder -> ColumnTypes.UUID),

    classOf[LocalDate] -> ( DateEncoderDecoder -> ColumnTypes.Date ),
    classOf[LocalDateTime] -> (TimestampEncoderDecoder.Instance -> ColumnTypes.Timestamp),
    classOf[DateTime] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[ReadableDateTime] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[ReadableInstant] -> (DateEncoderDecoder -> ColumnTypes.Date),

    classOf[ReadablePeriod] -> (PostgreSQLIntervalEncoderDecoder -> ColumnTypes.Interval),
    classOf[ReadableDuration] -> (PostgreSQLIntervalEncoderDecoder -> ColumnTypes.Interval),

    classOf[java.util.Date] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[java.sql.Date] -> ( DateEncoderDecoder -> ColumnTypes.Date ),
    classOf[java.sql.Time] -> ( SQLTimeEncoder -> ColumnTypes.Time ),
    classOf[java.sql.Timestamp] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[java.util.Calendar] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[java.util.GregorianCalendar] -> (TimestampWithTimezoneEncoderDecoder -> ColumnTypes.TimestampWithTimezone),
    classOf[Array[Byte]] -> ( ByteArrayEncoderDecoder -> ColumnTypes.ByteA ),
    classOf[ByteBuffer] -> ( ByteArrayEncoderDecoder -> ColumnTypes.ByteA ),
    classOf[ByteBuf] -> ( ByteArrayEncoderDecoder -> ColumnTypes.ByteA )
  )

  private final val classesSequence = (classOf[LocalTime] -> (TimeEncoderDecoder.Instance -> ColumnTypes.Time)) ::
    (classOf[ReadablePartial] -> (TimeEncoderDecoder.Instance -> ColumnTypes.Time)) ::
    classesSequence_

  private final val classes = classesSequence.toMap

  override def encode(value: Any): String = {

    if (value == null) {
      return null
    }

    value match {
      case Some(v) => encode(v)
      case None => null
      case _ => encodeValue(value)
    }

  }

  /**
   * Used to encode a value that is not null and not an Option.
   */
  private def encodeValue(value: Any): String = {

    val encoder = classes.get(value.getClass)

    if (encoder.isDefined) {
      encoder.get._1.encode(value)
    } else {
      value match {
        case i: java.lang.Iterable[_] => encodeArray(i.asScala)
        case i: Iterable[_] => encodeArray(i)
        case i: Array[_] => encodeArray(i.toIterable)
        case p: Product => encodeComposite(p)
        case _ => {
          classesSequence.find(entry => entry._1.isAssignableFrom(value.getClass)) match {
            case Some(parent) => parent._2._1.encode(value)
            case None => value.toString
          }
        }
      }

    }

  }

  private def encodeComposite(p: Product): String = {
    p.productIterator.map {
      item =>
        if (item == null || item == None) {
          "NULL"
        } else {
          if (shouldQuote(item)) {
            "\"" + encode(item).replace("\\", """\\""").replace("\"", """\"""") + "\""
          } else {
            encode(item)
          }
        }
    }.mkString("(", ",", ")")
  }

  private def encodeArray(collection: Iterable[_]): String =
    collection.map {
      item =>
        if (item == null || item == None)
          "NULL"
        else
          if (shouldQuote(item))
            "\"" + encode(item).replace("\\", """\\""").replace("\"", """\"""") + "\""
          else
            encode(item)
    }.mkString("{", ",", "}")

  private def shouldQuote(value: Any): Boolean = {
    value match {
      case n: java.lang.Number => false
      case n: Int => false
      case n: Short => false
      case n: Long => false
      case n: Float => false
      case n: Double => false
      case n: java.lang.Iterable[_] => false
      case n: Iterable[_] => false
      case n: Array[_] => false
      case Some(v) => shouldQuote(v)
      case _ => true
    }
  }

  override def kindOf(value: Any): Int =
    if ( value == null || value == None )
      0
    else
      value match {
        case Some(v) => kindOf(v)
        case v : String => ColumnTypes.Untyped
        case _ => {
          classes.get(value.getClass) match {
            case Some( entry ) => entry._2
            case None => ColumnTypes.Untyped
          }
        }
      }

}
