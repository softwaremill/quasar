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

package quasar.physical.rdbms.planner.sql

import slamdata.Predef._
import quasar.Data
import quasar.common.{JoinType, SortDir}
import quasar.physical.rdbms.model.ColumnType
import quasar.physical.rdbms.planner.sql.SqlExpr.Case.{Else, WhenThen}

import scalaz.{NonEmptyList, OneAnd}
import scalaz.NonEmptyList._
import scalaz.OneAnd._

sealed abstract class SqlExpr[T]

object SqlExpr extends SqlExprInstances {

  import Indirections._
  import Select._

  final case class Id[T](v: String, meta: Indirection) extends SqlExpr[T]

  final case class Refs[T](elems: Vector[T], m: Indirection) extends SqlExpr[T]
  final case class Unreferenced[T]() extends SqlExpr[T]
  final case class Null[T]() extends SqlExpr[T]
  final case class Obj[T](m: List[(T, T)]) extends SqlExpr[T]
  final case class IsNotNull[T](a1: T) extends SqlExpr[T]
  final case class ConcatStr[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Length[T](a1: T) extends SqlExpr[T]
  final case class IfNull[T](a: OneAnd[NonEmptyList, T]) extends SqlExpr[T]

  final case class ExprWithAlias[T](expr: T, alias: String) extends SqlExpr[T]

  final case class ExprPair[T](a: T, b: T, m: Indirection) extends SqlExpr[T]
  final case class Select[T](selection: Selection[T],
                             from: From[T],
                             join: Option[Join[T]],
                             filter: Option[Filter[T]],
                             groupBy: Option[GroupBy[T]],
                             orderBy: List[OrderBy[T]])
      extends SqlExpr[T]

  final case class From[T](v: T, alias: Id[T])
  final case class Join[T](v: T, keys: List[(T, T)], jType: JoinType, alias: Id[T])

  final case class Selection[T](v: T, alias: Option[Id[T]], meta: Indirection)
  final case class Table[T](name: String) extends SqlExpr[T]

  final case class NumericOp[T](op: String, left: T, right: T) extends SqlExpr[T]
  final case class Mod[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Pow[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Neg[T](a1: T) extends SqlExpr[T]

  final case class And[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Or[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Not[T](a1: T) extends SqlExpr[T]

  final case class Eq[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Neq[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Lt[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Lte[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Gt[T](a1: T, a2: T) extends SqlExpr[T]
  final case class Gte[T](a1: T, a2: T) extends SqlExpr[T]

  final case class DeleteKey[T](src: T, field: T) extends SqlExpr[T]
  final case class TypeOf[T](e: T) extends SqlExpr[T]
  final case class Coercion[T](t: ColumnType, e: T) extends SqlExpr[T]
  final case class ToArray[T](v: T) extends SqlExpr[T]

  final case class Constant[T](data: Data) extends SqlExpr[T]

  final case class UnaryFunction[T](t: UnaryFunctionType, e: T) extends SqlExpr[T]
  final case class BinaryFunction[T](t: BinaryFunctionType, a1: T, a2: T) extends SqlExpr[T]
  final case class TernaryFunction[T](t: TernaryFunctionType, a1: T, a2: T, a3: T) extends SqlExpr[T]

  final case class RegexMatches[T](a1: T, a2: T, caseInsensitive: Boolean) extends SqlExpr[T]

  final case class Limit[T](from: T, count: T) extends SqlExpr[T]
  final case class Offset[T](from: T, count: T) extends SqlExpr[T]

  object Select {
    final case class Filter[T](v: T)
    final case class RowIds[T]() extends SqlExpr[T]
    final case class AllCols[T]() extends SqlExpr[T]
    final case class WithIds[T](v: T) extends SqlExpr[T]
    final case class GroupBy[T](v: List[T])
    final case class OrderBy[T](v: T, sortDir: SortDir)
  }

  final case class Avg[T](v: T) extends SqlExpr[T]
  final case class Count[T](v: T) extends SqlExpr[T]
  final case class Max[T](v: T) extends SqlExpr[T]
  final case class Min[T](v: T) extends SqlExpr[T]
  final case class Sum[T](v: T) extends SqlExpr[T]
  final case class Distinct[T](v: T) extends SqlExpr[T]

  final case class Union[T](left: T, right: T) extends SqlExpr[T]

  object IfNull {
    def build[T](a1: T, a2: T, a3: T*): IfNull[T] = IfNull(oneAnd(a1, nels(a2, a3: _*)))
  }

  final case class Case[T](
                            whenThen: NonEmptyList[WhenThen[T]],
                            `else`: Else[T]
                          ) extends SqlExpr[T]

  object Case {
    final case class WhenThen[T](when: T, `then`: T)
    final case class Else[T](v: T)

    def build[T](a1: WhenThen[T], a: WhenThen[T]*)(`else`: Else[T]): Case[T] =
      Case(nels(a1, a: _*), `else`)
  }

  final case class ArrayUnwind[T](toUnwind: T) extends SqlExpr[T]

  final case class Time[T](d: T) extends SqlExpr[T]
  final case class Timestamp[T](d: T) extends SqlExpr[T]

  final case class DatePart[T](part: T, e: T) extends SqlExpr[T]

}

sealed trait UnaryFunctionType
case object StrLower extends UnaryFunctionType
case object StrUpper extends UnaryFunctionType
case object ToJson extends UnaryFunctionType

sealed trait BinaryFunctionType
case object StrSplit extends BinaryFunctionType
case object ArrayConcat extends BinaryFunctionType
case object Contains extends BinaryFunctionType

sealed trait TernaryFunctionType
case object Substring extends TernaryFunctionType
case object Search extends TernaryFunctionType
