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
import quasar.NameGenerator
import quasar.Planner.{PlannerErrorME}
import quasar.physical.rdbms.planner.sql.{SqlExpr => SQL}
import quasar.qscript, qscript.{MapFuncsCore => mfs, _}

import matryoshka._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._

final class EquiJoinPlanner[
T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator: PlannerErrorME]
  extends Planner[T, F, EquiJoin[T, ?]] {

  object KeyMetaId {
    def unapply(mf: FreeMap[T]): Boolean = mf.resume match {
      case -\/(
      MFC(
      mfs.ProjectKey(Embed(CoEnv(\/-(MFC(mfs.Meta(_))))),
      mfs.StrLit("id")))) =>
        true
      case _ => false
    }
  }

  def plan: AlgebraM[F, EquiJoin[T, ?], T[SQL]] = {
    case EquiJoin(Embed(_),
    lBranch,
    _,
    List((lKey, KeyMetaId())),
    joinType,
    combine) =>
      notImplemented("equijoin right branch collection", this)
    case EquiJoin(Embed(_),
    _,
    rBranch,
    List((KeyMetaId(), rKey)),
    joinType,
    combine) =>
      notImplemented("equijoin left branch collection", this)

  }
}
