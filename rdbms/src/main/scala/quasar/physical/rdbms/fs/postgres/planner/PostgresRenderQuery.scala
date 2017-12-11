/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms.fs.postgres.planner

import slamdata.Predef._
import quasar.common.SortDir.{Ascending, Descending}
import quasar.Data
import quasar.DataCodec
import quasar.DataCodec.Precise.TimeKey
import quasar.fp.ski._
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.fs.postgres._
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.sql.SqlExpr.Case._
import quasar.Planner.InternalError
import quasar.Planner.{NonRepresentableData, PlannerError}

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

object PostgresRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {
    val q = a.transCataT(transformRelationalOp).cataM(alg)

    a.project match {
      case s: Select[T[SqlExpr]] => q ∘ (s => s"select row_to_json(row) from $s row")
      case _ => q ∘ ("" ⊹ _)
    }
  }

  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  def buildJson(str: String): String =
    s"json_build_object($str)#>>'{}'"

  def transformRelationalOp[T[_[_]]: BirecursiveT](in: T[SqlExpr]): T[SqlExpr] = {

    def quotedStr(a: T[SqlExpr]): T[SqlExpr] =
      a.project match {
        case c@Constant(Data.Str(v)) =>
          Constant[T[SqlExpr]](Data.Str(s""""$v"""")).embed
        case other => other.embed
      }

    in.project match {
      case Eq(a1, a2) =>
        Eq[T[SqlExpr]](quotedStr(a1), quotedStr(a2)).embed
      case Lt(a1, a2) =>
        Lt[T[SqlExpr]](quotedStr(a1), quotedStr(a2)).embed
      case other =>
        other.embed
    }
  }

  val alg: AlgebraM[PlannerError \/ ?, SqlExpr, String] = {
    case Null() => "null".right
    case Unreferenced() =>
      s"unreferenced".right
    case SqlExpr.Id(v) =>
      s"""$v""".right
    case Table(v) =>
      v.right
    case AllCols() =>
      s"*".right
    case Refs(srcs) =>
      srcs match {
        case Vector(key, value) =>
          val valueStripped = value.stripPrefix("'").stripSuffix("'")
          s"""$key.$valueStripped""".right
        case key +: mid :+ last =>
          val firstValStripped = ~mid.headOption.map(_.stripPrefix("'").stripSuffix("'"))
          val midTail = mid.drop(1)
          val midStr = if (midTail.nonEmpty)
            s"->${midTail.map(e => s"$e").intercalate("->")}"
          else
            ""
          s"""$key.$firstValStripped$midStr->$last""".right
        case _ => InternalError.fromMsg(s"Cannot process Refs($srcs)").left
      }
    case Obj(m) =>
      buildJson(m.map {
        case (k, v) => s"'$k', $v"
      }.mkString(",")).right
    case Arr(l) =>
      l.mkString("[", ", ", "]").right
    case RegexMatches(str, pattern) =>
      s"($str ~ '$pattern')".right
    case IsNotNull(expr) =>
      s"($expr notnull)".right
    case ToJson(v) =>
      s"to_json($v)".right
    case IfNull(a) =>
      s"coalesce(${a.intercalate(", ")})".right
    case ExprWithAlias(expr: String, alias: String) =>
      (if (expr === alias) s"$expr" else s"""$expr as $alias""").right
    case ExprPair(expr1, expr2) =>
      s"$expr1, $expr2".right
    case ConcatStr(str1, str2)  =>
      s"$str1 || $str2".right
    case Time(expr) =>
      buildJson(s"""{ "$TimeKey": $expr }""").right
    case NumericOp(sym, left, right) => s"(($left)::text::numeric $sym ($right)::text::numeric)".right
    case Mod(a1, a2) => s"mod(($a1)::text::numeric, ($a2)::text::numeric)".right
    case Pow(a1, a2) => s"power(($a1)::text::numeric, ($a2)::text::numeric)".right
    case And(a1, a2) =>
      s"($a1 and $a2)".right
    case Or(a1, a2) =>
      s"($a1 or $a2)".right
    case Neg(str) => s"(-$str)".right
    case Eq(a1, a2) =>
      s"(($a1)::text = ($a2)::text)".right
    case Lt(a1, a2) =>
      s"(($a1)::text::numeric < ($a2)::text::numeric)".right
    case Lte(a1, a2) =>
      s"(($a1)::text::numeric <= ($a2)::text::numeric)".right
    case Gt(a1, a2) =>
      s"(($a1)::text::numeric > ($a2)::text::numeric)".right
    case Gte(a1, a2) =>
      s"(($a1)::text::numeric >= ($a2)::text::numeric)".right
    case WithIds(str)    => s"(row_number() over(), $str)".right
    case RowIds()        => "row_number() over()".right
    case Select(selection, from, jn, filterOpt, order) =>
      val filter = ~(filterOpt ∘ (f => s" where ${f.v}"))
      val join        = ~(jn ∘ (j =>
        j.joinType.fold(κ(" left outer"), κ("")) ⊹ s" join ${j.id.v}" ⊹ rowAlias(j.alias) ⊹
          " on " ⊹ j.pred))

      val orderStr = order.map { o =>
        val dirStr = o.sortDir match {
          case Ascending => "asc"
          case Descending => "desc"
        }
        s"${o.v} $dirStr"
      }.mkString(", ")

      val orderByStr = if (order.nonEmpty)
        s" order by $orderStr"
      else
        ""

      val fromExpr = s" from ${from.v} ${from.alias.v}"
      s"(select ${selection.v}$fromExpr$join$filter$orderByStr)".right
    case Constant(Data.Str(v)) =>
      val text = v.flatMap { case ''' => "''"; case iv => iv.toString }.self
      s"'$text'".right
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(w, t) => s"when $w then $t" }
      s"(case ${wts.intercalate(" ")} else ${e.v} end)".right
    case Coercion(t, e) => s"($e)::${t.mapToStringName}".right
  }
}
