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

import quasar.qscript
import qscript.{ReduceFunc, ReduceFuncs => RF}
import quasar.Planner.PlannerErrorME
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.physical.rdbms.planner.sql.SqlExpr._

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

final class ReduceFuncPlanner[T[_[_]]: CorecursiveT, F[_]: Applicative: PlannerErrorME] extends Planner[T, F, ReduceFunc] {

  def plan: AlgebraM[F, ReduceFunc, T[SqlExpr]] = {
    case RF.Arbitrary(a1)      => Min(a1).embed.η[F]
    case RF.Avg(a1)            => Avg(a1).embed.η[F]
    case RF.Count(a1)          => Count(a1).embed.η[F]
    case RF.First(a1)          => notImplemented[F, T[SqlExpr]]("First", this)
    case RF.Last(a1)           => notImplemented("Last", this)
    case RF.Max(a1)            => notImplemented("Max", this)
    case RF.Min(a1)            => notImplemented("Min", this)
    case RF.Sum(a1)            => notImplemented("Sum", this)
    case RF.UnshiftArray(a1)   => notImplemented("UnshiftArray", this)
    case RF.UnshiftMap(a1, a2) => notImplemented("UnshiftMap", this)
  }

}
