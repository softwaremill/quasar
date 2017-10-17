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
import quasar.Planner.PlannerError
import quasar.common.{PhaseResult, PhaseResultT, PhaseResultTell, PhaseResults}
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs.{FileSystemError, MonadFsErr}
import quasar.physical.rdbms.fs.postgres.Postgres
import quasar.physical.rdbms.model.Repr
import quasar.physical.rdbms.planner.Planner
import quasar.qscript.DiscoverPath
import quasar.{NameGenerator, RenderTreeT, Variables, queryPlan}
import quasar.sql.Sql

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix
import pathy.Path
import pathy.Path.{DirName, FileName, Sandboxed, dir, rootDir}
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait SqlExprSupport {

  type LP[A] = LogicalPlan[A]
  type QS[T[_[_]]] = quasar.physical.rdbms.model.QS[T]
  type QSM[T[_[_]], A] = QS[T]#M[A]

  val basePath
  : Path[Path.Abs, Path.Dir, Sandboxed] = rootDir[Sandboxed] </> dir("db")

  implicit val mtEitherWriter
  : MonadTell[EitherT[PhaseResultT[Id, ?], FileSystemError, ?],
    PhaseResults] =
    EitherT.monadListen[WriterT[Id, Vector[PhaseResult], ?],
      PhaseResults,
      FileSystemError](
      WriterT.writerTMonadListen[Id, Vector[PhaseResult]])

  def compileSqlToLP[M[_]: Monad: MonadFsErr: PhaseResultTell](
                                                                sql: Fix[Sql]): M[Fix[LP]] = {
    val (_, s) = queryPlan(sql, Variables.empty, basePath, 0L, None).run.run
    val lp = s.fold(
      e => scala.sys.error(e.shows),
      d => d.fold(e => scala.sys.error(e.shows), ι)
    )
    lp.point[M]
  }

  type EitherWriter[A] =
    EitherT[Writer[Vector[PhaseResult], ?], FileSystemError, A]

  val listContents: DiscoverPath.ListContents[EitherWriter] =
    dir =>
      (if (dir ≟ rootDir)
        Set(DirName("db").left[FileName],
          DirName("db1").left,
          DirName("db2").left)
      else
        Set(
          FileName("foo").right[DirName],
          FileName("foo2").right[DirName],
          FileName("bar").right,
          FileName("bar2").right
        )).point[EitherWriter]

  def plan(sql: Fix[Sql]) = {
    (compileSqlToLP[EitherWriter](sql) >>= (lp =>
      Postgres.lpToQScript(lp, listContents))).run.run._2.map(qsToRepr[Fix])
  }

  implicit def taskNameGenerator: NameGenerator[Task] =
    new NameGenerator[Task] {
      var counter = 0L
      def freshName = {
        val str = counter.toString
        counter += 1
        Task.delay(str)
      }
    }
  implicit val tmerr = new MonadError_[Task, PlannerError] {
    override def raiseError[A](e: PlannerError): Task[A] =
      Task.fail(new Exception(e.message))
    override def handleError[A](fa: Task[A])(
      f: PlannerError => Task[A]): Task[A] = fa
  }

  def qsToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
                                                                   cp: T[QSM[T, ?]]): Repr = {
    val planner = Planner[T, Task, QSM[T, ?]]
    cp.cataM(planner.plan).map(_.convertTo[Repr]).unsafePerformSync
  }

}
