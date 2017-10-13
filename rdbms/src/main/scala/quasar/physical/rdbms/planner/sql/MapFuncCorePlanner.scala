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
import matryoshka._
import quasar.NameGenerator
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.Planner
import quasar.qscript.MapFuncCore

import scalaz._

final class MapFuncCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: NameGenerator: PlannerErrorME]
  extends Planner[T, F, MapFuncCore[T, ?]] {

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[SqlExpr]] = {
    case other => PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unsupported MapFuncCore: $other"))
  }
}