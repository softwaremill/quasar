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
import quasar.Data
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.model.{BoolCol, ColumnType, ColumnarTable, DecCol, IntCol, JsonCol, JsonTable, NullCol, StringCol, TableModel}
import quasar.physical.rdbms.planner.sql._
import quasar.Planner.InternalError
import quasar.Planner.PlannerErrorME

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

sealed trait SqlExprType

case object JsonSqlExpr    extends SqlExprType
case object NumericSqlExpr extends SqlExprType
case object TextSqlExpr    extends SqlExprType
case object NullSqlExpr    extends SqlExprType
case object BoolSqlExpr    extends SqlExprType
case object UnknownSqlExpr extends SqlExprType

object SqlExprTyper {

  def colTypeToExprType(colType: ColumnType): SqlExprType = colType match {
    case JsonCol => JsonSqlExpr
    case IntCol => NumericSqlExpr
    case DecCol => NumericSqlExpr
    case StringCol => TextSqlExpr
    case NullCol => NullSqlExpr
    case BoolCol => BoolSqlExpr
  }

  def annotate[T[_[_]]: BirecursiveT, M[_]: Monad : PlannerErrorME](
      query: T[SqlExpr],
      model: TableModel): M[Cofree[SqlExpr, SqlExprType]] = {

    def colType(model: TableModel, colName: String): M[SqlExprType] = {
      model match {
        case JsonTable => (JsonSqlExpr: SqlExprType).η[M]
        case ColumnarTable(cols) =>
          cols.find(_.name === colName).map(c => colTypeToExprType(c.tpe).η[M])
            .getOrElse(PlannerErrorME[M]
              .raiseError(InternalError.fromMsg(s"Cannot determine type for column $colName with model $model")))
      }
    }

    def setType(tpe: SqlExprType, e: SqlExpr[Cofree[SqlExpr, SqlExprType]]) =
      Cofree(tpe, e).η[M]

    val alg: AlgebraM[M, SqlExpr, Cofree[SqlExpr, SqlExprType]] = {
      case e@Refs(elems) if elems.length > 2 =>
        setType(JsonSqlExpr, e)
      case e@Refs(Vector(_, columnNameExpr)) =>
        columnNameExpr.tail match {
          case Constant(Data.Str(colName)) =>
            colType(model, colName).map(t => Cofree(t, e))
          case _ =>
            setType(JsonSqlExpr, e)
        }
      case e@NumericOp(_, _, _) =>
        setType(NumericSqlExpr, e)
      case e@SqlExpr.Id(_) =>
        setType(TextSqlExpr, e)
      case e@SqlExpr.Null() =>
        setType(NullSqlExpr, e)
      case e@SqlExpr.Obj(_) =>
        setType(JsonSqlExpr, e)
      case e@IsNotNull(_) =>
        setType(BoolSqlExpr, e)
      case e@ConcatStr(_, _) =>
        setType(TextSqlExpr, e)
      case e@IfNull(l) =>
        setType(l.tail.head.head, e)
      case e@ExprWithAlias(expr, _) =>
        setType(expr.head, e)
      case e@ToJson(_) =>
        setType(JsonSqlExpr, e)
      case e: ArithmeticOp =>
        setType(NumericSqlExpr, e)
      case e: BoolOp =>
        setType(BoolSqlExpr, e)
      case e@Coercion(t, _) =>
        setType(colTypeToExprType(t), e)
      case e@Constant(Data.Str(_)) =>
        setType(TextSqlExpr, e)
      case e@Constant(Data.Bool(_)) =>
        setType(BoolSqlExpr, e)
      case e@Constant(Data.Null) =>
        setType(NullSqlExpr, e)
      case e@Constant(Data.Dec(_)) =>
        setType(NumericSqlExpr, e)
      case e@Constant(Data.Int(_)) =>
        setType(NumericSqlExpr, e)
      case e@Constant(Data.Obj(_)) =>
        setType(JsonSqlExpr, e)
      case e@UnaryFunction(StrLower, _) =>
        setType(TextSqlExpr, e)
      case e@UnaryFunction(StrUpper, _) =>
        setType(TextSqlExpr, e)
      case e@BinaryFunction(SplitStr, _, _) =>
        setType(TextSqlExpr, e)
      case e@TernaryFunction(Substring, _, _, _) =>
        setType(TextSqlExpr, e)
      case e@TernaryFunction(Search, _, _, _) =>
        setType(TextSqlExpr, e)
      case e@RegexMatches(_, _) =>
        setType(BoolSqlExpr, e)
      case e@Case(wt, _) =>
        setType(wt.head.`then`.head, e)
      case other =>
        setType(UnknownSqlExpr, other)
    }

    query.cataM(alg)
  }
}
