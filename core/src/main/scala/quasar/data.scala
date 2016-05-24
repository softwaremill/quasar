/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.fp._
import quasar.javascript.{Js}

import scala.Any

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz._, Scalaz._

sealed trait Data {
  def dataType: Type
  def toJs: Option[jscore.JsCore]
}

object Data {
  final case object Null extends Data {
    def dataType = Type.Null
    def toJs = jscore.Literal(Js.Null).some
  }

  final case class Str(value: String) extends Data {
    def dataType = Type.Str
    def toJs = jscore.Literal(Js.Str(value)).some
  }

  final case class Bool(value: Boolean) extends Data {
    def dataType = Type.Bool
    def toJs = jscore.Literal(Js.Bool(value)).some
  }
  val True = Bool(true)
  val False = Bool(false)

  sealed trait Number extends Data {
    override def equals(other: Any) = (this, other) match {
      case (Int(v1), Number(v2)) => BigDecimal(v1) == v2
      case (Dec(v1), Number(v2)) => v1 == v2
      case _                     => false
    }
  }
  object Number {
    def unapply(value: Data): Option[BigDecimal] = value match {
      case Int(value) => Some(BigDecimal(value))
      case Dec(value) => Some(value)
      case _ => None
    }
  }
  final case class Dec(value: BigDecimal) extends Number {
    def dataType = Type.Dec
    def toJs = jscore.Literal(Js.Num(value.doubleValue, true)).some
  }
  final case class Int(value: BigInt) extends Number {
    def dataType = Type.Int
    def toJs = jscore.Literal(Js.Num(value.doubleValue, false)).some
  }

  final case class Obj(value: ListMap[String, Data]) extends Data {
    def dataType = Type.Obj(value ∘ (Type.Const(_)), None)

   def toJs =
     value.toList.map(_.bimap(jscore.Name(_), _.toJs))
          .toListMap.sequence.map(jscore.Obj(_))
  }

  final case class Arr(value: List[Data]) extends Data {
    def dataType = Type.Arr(value ∘ (Type.Const(_)))
    def toJs = value.traverse(_.toJs).map(jscore.Arr(_))
  }

  final case class Set(value: List[Data]) extends Data {
    def dataType = value.foldLeft[Type](Type.Bottom)((acc, d) => Type.lub(acc, d.dataType))
    def toJs = None
  }

  final case class Timestamp(value: Instant) extends Data {
    def dataType = Type.Timestamp
    def toJs = jscore.Call(jscore.ident("ISODate"), List(jscore.Literal(Js.Str(value.toString)))).some
  }

  final case class Date(value: LocalDate) extends Data {
    def dataType = Type.Date
    def toJs = jscore.Call(jscore.ident("ISODate"), List(jscore.Literal(Js.Str(value.toString)))).some
  }

  final case class Time(value: LocalTime) extends Data {
    def dataType = Type.Time
    def toJs = jscore.Literal(Js.Str(value.toString)).some
  }

  final case class Interval(value: Duration) extends Data {
    def dataType = Type.Interval
    def toJs = jscore.Literal(Js.Num(value.getSeconds*1000 + value.getNano*1e-6, true)).some
  }

  final case class Binary(value: ImmutableArray[Byte]) extends Data {
    def dataType = Type.Binary
    def toJs = jscore.Call(jscore.ident("BinData"), List(
      jscore.Literal(Js.Num(0, false)),
      jscore.Literal(Js.Str(base64)))).some

    def base64: String = new sun.misc.BASE64Encoder().encode(value.toArray)

    override def toString = "Binary(Array[Byte](" + value.mkString(", ") + "))"

    override def equals(that: Any): Boolean = that match {
      case Binary(value2) => value ≟ value2
      case _ => false
    }
    override def hashCode = java.util.Arrays.hashCode(value.toArray[Byte])
  }
  object Binary {
    def apply(array: Array[Byte]): Binary = Binary(ImmutableArray.fromArray(array))
  }

  final case class Id(value: String) extends Data {
    def dataType = Type.Id
    def toJs = jscore.Call(jscore.ident("ObjectId"), List(jscore.Literal(Js.Str(value)))).some
  }

  /**
   An object to represent any value that might come from a backend, but that
   we either don't know about or can't represent in this ADT. We represent it
   with JS's `undefined`, just because no other value will ever be translated
   that way.
   */
  final case object NA extends Data {
    def dataType = Type.Bottom
    def toJs = jscore.ident(Js.Undefined.ident).some
  }

  final class Comparable private (val value: Data) extends scala.AnyVal

  object Comparable {
    def apply(data: Data): Option[Comparable] =
      some(data)
        .filter(d => Type.Comparable contains d.dataType)
        .map(new Comparable(_))

    def partialCompare(a: Comparable, b: Comparable): Option[Ordering] = {
      (a.value, b.value) match {
        case (Int(x), Int(y))             => Some(x cmp y)
        case (Dec(x), Dec(y))             => Some(x cmp y)
        case (Str(x), Str(y))             => Some(x cmp y)
        case (Bool(x), Bool(y))           => Some(x cmp y)
        case (Date(x), Date(y))           => Some(Ordering.fromInt(x compareTo y))
        case (Time(x), Time(y))           => Some(Ordering.fromInt(x compareTo y))
        case (Timestamp(x), Timestamp(y)) => Some(Ordering.fromInt(x compareTo y))
        case (Interval(x), Interval(y))   => Some(Ordering.fromInt(x compareTo y))
        case _                            => None
      }
    }

    def min(a: Comparable, b: Comparable): Option[Comparable] = {
      partialCompare(a, b) map {
        case Ordering.LT => a
        case Ordering.EQ => a
        case Ordering.GT => b
      }
    }

    def max(a: Comparable, b: Comparable): Option[Comparable] = {
      partialCompare(a, b) map {
        case Ordering.LT => b
        case Ordering.EQ => a
        case Ordering.GT => a
      }
    }
  }

  implicit val dataShow: Show[Data] =
    Show.showFromToString
}
