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
import quasar.fp.ski._
import quasar.Planner._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.qscript._
import matryoshka.{AlgebraM, BirecursiveT, ShowT}

import scalaz._

trait Planner[QS[_], F[_]] extends Serializable {
  import Planner._

  def plan(expr: SqlExprBuilder): AlgebraM[F, QS, Repr]
}

object Planner {
  type Repr = quasar.physical.rdbms.model.Repr

  def apply[QS[_], F[_]](implicit P: Planner[QS, F]): Planner[QS, F] = P

  implicit def deadEnd[F[_]: PlannerErrorME]: Planner[Const[DeadEnd, ?], F] =
    unreachable("deadEnd")

  implicit def read[A, F[_]: PlannerErrorME]: Planner[Const[Read[A], ?], F] =
    unreachable("read")

  implicit def shiftedReadPath[F[_]: PlannerErrorME]
    : Planner[Const[ShiftedRead[ADir], ?], F] =
    unreachable("shifted read of a dir")

  implicit def projectBucket[T[_[_]], F[_]: PlannerErrorME]
    : Planner[ProjectBucket[T, ?], F] = unreachable("projectBucket")

  implicit def thetaJoin[T[_[_]], F[_]: PlannerErrorME]
    : Planner[ThetaJoin[T, ?], F] = unreachable("thetajoin")

  implicit def shiftedread[F[_]: Applicative]
    : Planner[Const[ShiftedRead[AFile], ?], F] = new ShiftedReadPlanner[F]

  implicit def qscriptCore[T[_[_]]: BirecursiveT: ShowT, F[_]: PlannerErrorME]
    : Planner[QScriptCore[T, ?], F] = unreachable("TODO")

  implicit def equiJoin[T[_[_]]: BirecursiveT: ShowT, F[_]: PlannerErrorME]
    : Planner[EquiJoin[T, ?], F] = unreachable("TODO")

  implicit def coproduct[QS[_], G[_], F[_]](
      implicit F: Planner[QS, F],
      G: Planner[G, F]): Planner[Coproduct[QS, G, ?], F] =
    new Planner[Coproduct[QS, G, ?], F] {
      def plan(expr: SqlExprBuilder): AlgebraM[F, Coproduct[QS, G, ?], Repr] =
        _.run.fold(F.plan(expr), G.plan(expr))
    }

  private def unreachable[QS[_], F[_]: PlannerErrorME](
      what: String): Planner[QS, F] =
    new Planner[QS, F] {
      override def plan(expr: SqlExprBuilder): AlgebraM[F, QS, Repr] =
        κ(
          PlannerErrorME[F].raiseError(
            InternalError.fromMsg(s"unreachable $what")))
    }

}
