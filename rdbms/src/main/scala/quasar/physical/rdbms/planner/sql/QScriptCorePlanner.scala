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

package quasar.physical.rdbms.planner.sql

import slamdata.Predef._
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.Planner
import quasar.{NameGenerator, qscript}
import quasar.qscript.QScriptCore

import matryoshka._
import scalaz.Monad
import scalaz.syntax.applicative._

class QScriptCorePlanner[T[_[_]]: CorecursiveT, F[_]: Monad: NameGenerator: PlannerErrorME] extends Planner[T, F, QScriptCore[T, ?]] {

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(src, _) =>
      src.point[F] // TODO
    case _ =>
      PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unreachable QScriptCore"))
  }
}