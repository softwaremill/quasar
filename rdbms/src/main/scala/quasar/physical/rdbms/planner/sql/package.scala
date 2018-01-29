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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.NameGenerator
import quasar.physical.rdbms.planner.sql.SqlExpr.Id
import quasar.Planner.{InternalError, PlannerErrorME}
import sql.SqlExpr._
import Select._

import matryoshka._
import matryoshka.implicits._
import scalaz.Functor
import scalaz.syntax.functor._

package object sql {

  def genId[T, F[_]: Functor: NameGenerator](m: Indirections.Indirection): F[Id[T]] =
    NameGenerator[F].prefixedName("_") ∘ (Id(_, m))

  def unexpected[F[_]: PlannerErrorME, A](name: String): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unexpected $name"))

  def unsupported[F[_]: PlannerErrorME, A](name: String): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"Unsupported $name"))

  def *[T[_[_]]: BirecursiveT] : T[SqlExpr] = AllCols[T[SqlExpr]]().embed
  /**
    * Use to convert expressions like (select _id from (select ....) _id) is effectively equal to
    * (select * from (select ....) _id) to avoid problems with record types.
    */
  def idToWildcard[T[_[_]]: BirecursiveT](e: T[SqlExpr]): T[SqlExpr] = {
    e.project match {
      case Id(_, _) => *[T]
      case ExprPair(a, b, _) =>
        (a.project, b.project) match {
          case (Id(_, _), Id(_, _)) => *[T]
          case _ => e
        }
      case _ => e
    }
  }
}
