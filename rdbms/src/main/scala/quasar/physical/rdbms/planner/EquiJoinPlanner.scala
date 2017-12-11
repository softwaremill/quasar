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
import quasar.common.JoinType
import quasar.fp.ski._
import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.physical.rdbms.planner.sql._
import quasar.physical.rdbms.planner.sql.{SqlExpr => SQL}
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.qscript
import qscript.{MapFuncsCore => mfs}
import qscript._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import quasar.common.JoinType
import quasar.contrib.pathy.AFile
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.common.TablePath._

import scalaz._
import Scalaz._

final class EquiJoinPlanner[
    T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator: PlannerErrorME]
    extends Planner[T, F, EquiJoin[T, ?]] {

  lazy val tPlan: AlgebraM[F, QScriptTotal[T, ?], T[SQL]] =
    Planner[T, F, QScriptTotal[T, ?]].plan

  lazy val mfPlan: AlgebraM[F, MapFunc[T, ?], T[SQL]] =
    Planner.mapFuncPlanner[T, F].plan

  val sqlNull: T[SqlExpr] = SQL.Null[T[SQL]]().embed

  val QC = Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

  object CShiftedRead {
    def unapply[F[_], A](
                          fa: F[A]
                        )(implicit
                          C: Const[ShiftedRead[AFile], ?] :<: F
                        ): Option[Const[ShiftedRead[AFile], A]] =
      C.prj(fa)
  }

  object MetaGuard {
    object Meta {
      def unapply[A](mf: FreeMapA[T, A]): Boolean =
        (mf.resume.swap.toOption >>= { case MFC(mfs.Meta(_)) => ().some; case _ => none }).isDefined
    }

    def unapply[A](mf: FreeMapA[T, A]): Boolean = (
      mf.resume.swap.toOption >>= { case MFC(mfs.Guard(Meta(), _, _, _)) => ().some; case _ => none }
      ).isDefined
  }

    object TableBranch {
    def unapply(qs: FreeQS[T]): Option[TablePath] = (qs match {
      case Embed(CoEnv(\/-(CShiftedRead(c))))                              => c.some
      case Embed(CoEnv(\/-(QC(
       qscript.Filter(Embed(CoEnv(\/-(CShiftedRead(c)))), MetaGuard()))))) => c.some
      case _                                                               => none
    }) ∘ (c =>  TablePath.create(c.getConst.path))
  }

  def keyJoin(
      branch: FreeQS[T],
      rKey: FreeMap[T],
      lKey: FreeMap[T],
      combine: JoinFunc[T],
      tablePath: TablePath,
      side: JoinSide,
      joinType: LookupJoinType
  ): F[T[SQL]] = {

    for {
      idL <- genId[T[SQL], F]
      idR <- genId[T[SQL], F]
      from <- branch.cataM(interpretM(κ(sqlNull.η[F]), tPlan))
      k <- {
        for {
          lkExpr <- lKey.cataM(interpretM(κ(idL.embed.η[F]), mfPlan))
          rkExpr <- rKey.cataM(interpretM(κ(idR.embed.η[F]), mfPlan))
        }
          yield {
            SQL.Eq[T[SQL]](lkExpr, rkExpr).embed
          }

      }
      selection <- combine.cataM(interpretM({
        case `side` => idL.embed.η[F]
        case _      => idR.embed.η[F]
      }, mfPlan))
    } yield
      Select(Selection(selection, alias = none),
             From(from, idL),
             LookupJoin(SQL.Id(tablePath.shows), idR.some, k, joinType).some,
             filter = none,
             orderBy = nil).embed
  }

  def plan: AlgebraM[F, EquiJoin[T, ?], T[SQL]] = {
    case EquiJoin(_, lBranch, TableBranch(tp), List((lKey, rKey)), joinType, combine) =>
      joinType match {
        case JoinType.Inner =>
          keyJoin(lBranch, rKey, lKey, combine, tp, LeftSide, JoinType.Inner.right)
        case JoinType.LeftOuter =>
          keyJoin(lBranch, rKey, lKey, combine, tp, LeftSide, JoinType.LeftOuter.left)
        case other =>
          notImplemented(s"$other", this)
      }
    case EquiJoin(_, TableBranch(tp), rBranch, List((lKey, rKey)), joinType, combine) =>
      joinType match {
        case JoinType.Inner =>
          keyJoin(rBranch, rKey, lKey, combine, tp, RightSide, JoinType.Inner.right)
        case JoinType.RightOuter =>
          keyJoin(rBranch, rKey, lKey, combine, tp, RightSide, JoinType.LeftOuter.left)
        case other =>
          notImplemented(s"$other", this)
      }
    case other =>
      notImplemented(s"$other", this)
  }
}
