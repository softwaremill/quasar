/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.std

import slamdata.Predef._
import quasar.{Data, Func, UnaryFunc, BinaryFunc, Type, Mapping}
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import scala.math.BigDecimal.RoundingMode

import matryoshka._
import scalaz._, Scalaz._, Validation.success
import shapeless._

trait MathLib extends Library {
  private val MathRel = Type.Numeric ⨿ Type.Interval
  private val MathAbs = Type.Numeric ⨿ Type.Interval ⨿ Type.Temporal

  // TODO[monocle]: Unit unapply needs to do Boolean instead of Option[Unit]
  // val Zero = Prism.partial[Data, Unit] {
  //   case Data.Number(v) if v ≟ 0 => ()
  // } (κ(Data.Int(0)))
  // Be careful when using this. Zero() creates an Int(0), but it *matches* Dec(0) too.
  object Zero {
    def apply() = Data.Int(0)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v ≟ 0 => true
      case _                       => false
    }
  }
  object One {
    def apply() = Data.Int(1)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v ≟ 1 => true
      case _                       => false
    }
  }

  object ZeroF {
    def apply() = Constant(Zero())
    def unapply[A](obj: LP[A]): Boolean = obj match {
      case Constant(Zero()) => true
      case _                 => false
    }
  }
  object OneF {
    def apply() = Constant(One())
    def unapply[A](obj: LP[A]): Boolean = obj match {
      case Constant(One()) => true
      case _                => false
    }
  }

  object TZero {
    def apply() = Type.Const(Zero())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(Zero()) => true
      case _                  => false
    }
  }
  object TOne {
    def apply() = Type.Const(One())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(One()) => true
      case _                 => false
    }
  }

  /** Adds two numeric values, promoting to decimal if either operand is
    * decimal.
    */
  val Add = BinaryFunc(
    Mapping,
    "Adds two numeric or temporal values",
    MathAbs,
    Func.Input2(MathAbs, MathRel),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(ZeroF()))) => x.some
          case Invoke(_, Sized(Embed(ZeroF()), Embed(x))) => x.some
          case _                                           => None
        }
    },
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))             => Type.Const(Data.Int(v1 + v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2)))       => Type.Const(Data.Dec(v1 + v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Interval(v2)))  => Type.Const(Data.Timestamp(v1.plus(v2)))
      case Sized(t, Type.Interval) if Type.Timestamp.contains(t)                 => Type.Timestamp
      case Sized(t, Type.Interval) if Type.Date.contains(t)                      => Type.Date
      case Sized(t, Type.Interval) if Type.Time.contains(t)                      => Type.Time
      case Sized(t1, t2)
        if Type.Interval.contains(t1) && Type.Interval.contains(t2)              => Type.Interval
    } ||| numericWidening,
    partialUntyperOV[nat._2](t => Type.typecheck(Type.Temporal ⨿ Type.Interval, t).fold(
      κ(t match {
        case Type.Int                      => Some(success(Func.Input2(Type.Int, Type.Int)))
        case t if Type.Numeric.contains(t) => Some(success(Func.Input2(Type.Numeric, Type.Numeric)))
        case _                             => None
      }),
      κ(Some(success(Func.Input2(t, Type.Interval)))))))

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = BinaryFunc(
    Mapping,
    "Multiplies two numeric values or one interval and one numeric value",
    MathRel,
    Func.Input2(MathRel, MathRel),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case Invoke(_, Sized(Embed(OneF()), Embed(x))) => x.some
          case _                                          => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(TZero(), t) if Type.Numeric.contains(t) => Type.Const(Data.Dec(0))
      case Sized(t, TZero()) if Type.Numeric.contains(t) => Type.Const(Data.Dec(0))

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => Type.Const(Data.Int(v1 * v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Dec(v1 * v2))

      // TODO: handle interval multiplied by Dec (not provided by threeten). See SD-582.
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Int(v2))) => Type.Const(Data.Interval(v1.multipliedBy(v2.longValue)))
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Interval(v2.multipliedBy(v1.longValue)))
      case Sized(Type.Interval, Type.Int) => Type.Interval
      case Sized(Type.Int, Type.Interval) => Type.Interval
    }) ||| numericWidening,
    partialUntyper[nat._2] {
      case Type.Interval => Func.Input2(Type.Int ⨿ Type.Interval, Type.Int ⨿ Type.Interval)
      case Type.Int => Func.Input2(Type.Int, Type.Int)
      case Type.Dec => Func.Input2(Type.Numeric, Type.Numeric)
      case _        => Func.Input2(MathRel, MathRel)
    })

  val Power = BinaryFunc(
    Mapping,
    "Raises the first argument to the power of the second",
    Type.Numeric,
    Func.Input2(Type.Numeric, Type.Numeric),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case _                                         => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(_, TZero()) => TOne()
      case Sized(v1, TOne()) => v1
      case Sized(TZero(), _) => TZero()

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2))) if v2.isValidInt    => Type.Const(Data.Int(v1.pow(v2.toInt)))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Int(v2))) if v2.isValidInt => Type.Const(Data.Dec(v1.pow(v2.toInt)))
    }) ||| numericWidening,
    partialUntyper[nat._2] {
      case Type.Int => Func.Input2(Type.Int, Type.Int)
      case Type.Dec => Func.Input2(Type.Numeric, Type.Numeric)
    })

  /** Subtracts one value from another, promoting to decimal if either operand
    * is decimal.
    */
  val Subtract = BinaryFunc(
    Mapping,
    "Subtracts two numeric or temporal values",
    MathAbs,
    Func.Input2(MathAbs, MathAbs),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(ZeroF()))) => x.some
          case Invoke(_, Sized(Embed(ZeroF()), x))        => Negate(x).some
          case _                                           => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(v1, TZero()) if Type.Numeric.contains(v1) => v1

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => Type.Const(Data.Int(v1 - v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Dec(v1 - v2))
      case Sized(t1, t2)
        if (Type.Temporal.contains(t1) && t1.contains(t2)) || (Type.Temporal.contains(t2) && t2.contains(t1))
                                                                           => Type.Interval
      case Sized(t, Type.Interval) if Type.Temporal.contains(t)            => t
    }) ||| numericWidening,
    partialUntyperOV[nat._2] { t => Type.typecheck(Type.Temporal, t).fold(
      κ(Type.typecheck(Type.Interval, t).fold(
        κ(t match {
          case Type.Int                      => Some(success(Func.Input2(Type.Int    , Type.Int    )))
          case t if Type.Numeric.contains(t) => Some(success(Func.Input2(Type.Numeric, Type.Numeric)))
          case _                             => None
        }),
        κ(Some(success(Func.Input2(Type.Temporal, Type.Temporal)))))),
      κ(Some(success(Func.Input2(t, Type.Interval)))))})

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = BinaryFunc(
    Mapping,
    "Divides one numeric or interval value by another (non-zero) numeric value",
    Type.Dec ⨿ Type.Interval,
    Func.Input2(MathRel, MathRel),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case _                                         => None
        }
    },
    partialTyperV[nat._2] {
      case Sized(v1, TOne())  => success(v1)

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))
        if v2 != BigInt(0)                                                => success(Type.Const(Data.Dec(BigDecimal(v1) / BigDecimal(v2))))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2)))
        if v2 != BigDecimal(0)                                            => success(Type.Const(Data.Dec(v1 / v2)))

      // TODO: handle interval divided by Dec (not provided by threeten). See SD-582.
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Int(v2))) => success(Type.Const(Data.Interval(v1.dividedBy(v2.longValue))))
      case Sized(t1, t2)
        if Type.Interval.contains(t1) && Type.Int.contains(t2)            => success(Type.Interval)
      case Sized(t1, t2)
        if Type.Interval.contains(t1) && Type.Interval.contains(t2)       => success(Type.Dec)
      case Sized(t1, t2)
        if Type.Numeric.contains(t1) && Type.Numeric.contains(t2)         => success(Type.Dec)
    },
    untyper[nat._2](t => Type.typecheck(Type.Interval, t).fold(
      κ(success(Func.Input2(MathRel, MathRel))),
      κ(success(Func.Input2(Type.Interval, Type.Int))))))

  /**
   * Aka "unary minus".
   */
  val Negate = UnaryFunc(
    Mapping,
    "Reverses the sign of a numeric or interval value",
    MathRel,
    Func.Input1(MathRel),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(-v)))
      case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(-v)))
      case Sized(Type.Const(Data.Interval(v))) => success(Type.Const(Data.Interval(v.negated)))

      case Sized(t) if (Type.Numeric ⨿ Type.Interval) contains t => success(t)
    },
    untyper[nat._1] {
      case t             => success(Func.Input1(t))
    })

  val Abs = UnaryFunc(
    Mapping,
    "Returns the absolute value of a numeric or interval value",
    MathRel,
    Func.Input1(MathRel),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v.abs)))
      case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(v.abs)))
      case Sized(Type.Const(Data.Interval(v))) => success(Type.Const(Data.Interval(v.abs)))

      case Sized(t) if (Type.Numeric ⨿ Type.Interval) contains t => success(t)
    },
    untyper[nat._1] {
      case t             => success(Func.Input1(t))
    })

    val Ceil = UnaryFunc(
      Mapping,
      "Returns the nearest integer greater than or equal to a numeric value",
      Type.Int,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v))) => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v))) => success(Type.Const(Data.Int(v.setScale(0, RoundingMode.CEILING).toBigInt())))
      },
      basicUntyper)

    val Floor = UnaryFunc(
      Mapping,
      "Returns the nearest integer less than or equal to a numeric value",
      Type.Int,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v))) => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v))) => success(Type.Const(Data.Int(v.setScale(0, RoundingMode.FLOOR).toBigInt)))
      },
      basicUntyper)

    val Trunc = UnaryFunc(
      Mapping,
      "Truncates a numeric value towards zero",
      Type.Int,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Int(v.toBigInt)))
      },
      basicUntyper)

    val Round = UnaryFunc(
      Mapping,
      "Rounds a numeric value to the closest integer, utilizing a half-even strategy",
      Type.Int,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v))) => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v))) => success(Type.Const(Data.Int(v.setScale(0, RoundingMode.HALF_EVEN).toBigInt)))
      },
      basicUntyper)

    val FloorScale = BinaryFunc(
      Mapping,
      "Returns the nearest number less-than or equal-to a given number, with the specified number of decimal digits",
      Type.Numeric,
      Func.Input2(Type.Numeric, Type.Int),
      noSimplification,
      (partialTyperV[nat._2] {
        case Sized(v @ Type.Const(Data.Int(_)), Type.Const(Data.Int(s))) if s >= 0 => success(v)
        case Sized(Type.Const(Data.Int(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(BigDecimal(v).setScale(s.toInt, RoundingMode.FLOOR))))
        case Sized(Type.Const(Data.Dec(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(v.setScale(s.toInt, RoundingMode.FLOOR))))

        case Sized(t1, t2) if Type.Numeric.contains(t1) && Type.Numeric.contains(t2) => success(t1)
      }),
      partialUntyper[nat._2] {
        case t => Func.Input2(t, Type.Int)
      })

    val CeilScale = BinaryFunc(
      Mapping,
      "Returns the nearest number greater-than or equal-to a given number, with the specified number of decimal digits",
      Type.Numeric,
      Func.Input2(Type.Numeric, Type.Int),
      noSimplification,
      (partialTyperV[nat._2] {
        case Sized(v @ Type.Const(Data.Int(_)), Type.Const(Data.Int(s))) if s >= 0 => success(v)
        case Sized(Type.Const(Data.Int(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(BigDecimal(v).setScale(s.toInt, RoundingMode.CEILING))))
        case Sized(Type.Const(Data.Dec(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(v.setScale(s.toInt, RoundingMode.CEILING))))

        case Sized(t1, t2) if Type.Numeric.contains(t1) && Type.Numeric.contains(t2) => success(t1)
      }),
      partialUntyper[nat._2] {
        case t => Func.Input2(t, Type.Int)
      })

    val RoundScale = BinaryFunc(
      Mapping,
      "Returns the nearest number to a given number with the specified number of decimal digits",
      Type.Numeric,
      Func.Input2(Type.Numeric, Type.Int),
      noSimplification,
      (partialTyperV[nat._2] {
        case Sized(v @ Type.Const(Data.Int(_)), Type.Const(Data.Int(s))) if s >= 0 => success(v)
        case Sized(Type.Const(Data.Int(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(BigDecimal(v).setScale(s.toInt, RoundingMode.HALF_EVEN))))
        case Sized(Type.Const(Data.Dec(v)), Type.Const(Data.Int(s))) => success(Type.Const(Data.Dec(v.setScale(s.toInt, RoundingMode.HALF_EVEN))))

        case Sized(t1, t2) if Type.Numeric.contains(t1) && Type.Numeric.contains(t2) => success(t1)
      }),
      partialUntyper[nat._2] {
        case t => Func.Input2(t, Type.Int)
      })

  // Note: there are 2 interpretations of `%` which return different values for negative numbers.
  // Depending on the interpretation `-5.5 % 1` can either be `-0.5` or `0.5`.
  // Generally, the first interpretation seems to be referred to as "remainder" and the 2nd as "modulo".
  // Java/scala and PostgreSQL all use the remainder interpretation, so we use it here too.
  // However, since PostgreSQL uses the function name `mod` as an alias for `%` while using the term
  // remainder in its description we keep the term `Modulo` around.
  val Modulo = BinaryFunc(
    Mapping,
    "Finds the remainder of one number divided by another",
    MathRel,
    Func.Input2(MathRel, Type.Numeric),
    noSimplification,
    (partialTyperV[nat._2] {
      case Sized(v1, TOne()) if Type.Int.contains(v1)                      => success(TZero())
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))
        if v2 != BigInt(0)                                                 => success(Type.Const(Data.Int(v1 % v2)))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2)))
        if v2 != BigDecimal(0)                                             => success(Type.Const(Data.Dec(v1 % v2)))
      case Sized(Type.Interval, t) if Type.Numeric.contains(t)             => success(Type.Interval)
      case Sized(t1, t2)
        if Type.Int.contains(t1) && Type.Int.contains(t2)                  => success(Type.Int)
      case Sized(t1, t2)
        if Type.Dec.contains(t1) && Type.Dec.contains(t2)                  => success(Type.Dec)
      case Sized(t1, t2)
        if Type.Numeric.contains(t1) && Type.Numeric.contains(t2)          => success(Type.Numeric)
    }),
    partialUntyper[nat._2] {
      case Type.Int => Func.Input2(Type.Int, Type.Int)
      case t if Type.Numeric.contains(t) => Func.Input2(Type.Numeric, Type.Numeric)
      case Type.Interval => Func.Input2(Type.Interval, Type.Numeric)
    })
}

object MathLib extends MathLib
