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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.common.SortDir
import quasar.fp.ski._
import quasar.{NameGenerator, qscript}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.{SqlExpr, genId}
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.qscript.{FreeMap, MapFunc, QScriptCore}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.Scalaz._
import scalaz._

class QScriptCorePlanner[T[_[_]]: BirecursiveT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  def * : T[SqlExpr] = AllCols[T[SqlExpr]]().embed

  def processFreeMap(f: FreeMap[T],
                     alias: SqlExpr.Id[T[SqlExpr]]): F[T[SqlExpr]] =
    f.cataM(interpretM(κ(alias.embed.η[F]), mapFuncPlanner.plan))

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(src, f) =>
      for {
        fromAlias <- genId[T[SqlExpr], F]
        selection <- {
          val holeF = quasar.qscript.HoleF[T]
          f match {
            case `holeF` => *.η[F]
            case _ => processFreeMap(f, fromAlias).map { ff =>
              ff.project match {
                case SqlExpr.Id(_) => *
                case other => other.embed
              }
            }
          }
        }
      } yield
        Select(
          Selection(selection, none),
          From(src, fromAlias),
          join = none,
          filter = none,
          orderBy = nil
        ).embed
    case qscript.Sort(src, bucket, order) =>
      def createOrderBy(id: SqlExpr.Id[T[SqlExpr]]):
      ((FreeMap[T], SortDir)) => F[OrderBy[T[SqlExpr]]] = {
        case (qs, dir) =>
          processFreeMap(qs, id).map { expr =>
            OrderBy(expr, dir)
          }
      }

      for {
        fromAlias <- genId[T[SqlExpr], F]
        orderByExprs <- order.traverse(createOrderBy(fromAlias))
        bucketExprs <- bucket.map((_, orderByExprs.head.sortDir)).traverse(createOrderBy(fromAlias))
      }
        yield {
          Select(
            Selection(*, none),
            From(src, fromAlias),
            join = none,
            filter = none,
            orderBy = bucketExprs ++ orderByExprs.toList
          ).embed
        }

    case qscript.Filter(src, f) =>
      src.project match {
        case s@Select(_, From(_, initialFromAlias), _, initialFilter, _) =>
          val injectedFilterExpr = processFreeMap(f, initialFromAlias)
          injectedFilterExpr.map { fe =>
            val finalFilterExpr = initialFilter.map(i => And[T[SqlExpr]](i.v, fe).embed).getOrElse(fe)
            s.copy(filter = Some(Filter[T[SqlExpr]](finalFilterExpr))).embed
          }
        case other =>
          for {
            fromAlias <- genId[T[SqlExpr], F]
            filterExp <- processFreeMap(f, fromAlias)
          } yield {
          Select(
            Selection(*, none),
            From(src, fromAlias),
            join = none,
            Some(Filter(filterExp)),
            orderBy = nil
          ).embed
      }
    }
    case qscript.Unreferenced() =>
      Unreferenced[T[SqlExpr]]().embed.η[F]

    case other =>
      PlannerErrorME[F].raiseError(
        InternalError.fromMsg(s"unsupported QScriptCore: $other"))
  }
}
