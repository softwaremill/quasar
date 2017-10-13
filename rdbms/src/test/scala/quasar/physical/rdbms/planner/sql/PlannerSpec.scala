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
import quasar._
import quasar.common.{PhaseResult, PhaseResultT, PhaseResultTell, PhaseResults}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs.{FileSystemError, MonadFsErr}
import quasar.{NameGenerator, Qspec, Variables}
import quasar.physical.rdbms.fs.postgres.Postgres
import quasar.physical.rdbms.model.Repr
import quasar.physical.rdbms.planner.Planner
import quasar.Planner.PlannerError
import quasar.qscript._
import quasar.sql._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import org.specs2.execute.NoDetails
import org.specs2.matcher._
import pathy.Path
import pathy.Path._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

class PlannerSpec extends Qspec {

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

  import quasar.fp.free._

  implicit lazy val taskNameGenerator: NameGenerator[Task] =
    new NameGenerator[Task] {
      var counter = 0L
      def freshName = {
        val str = counter.toString
        counter += 1
        Task.delay(str)
      }
    }

  def runTest[A, S[_]](f: Free[S, A])(implicit S: S :<: Task): A = {
    f.foldMap(injectNT[S, Task]).unsafePerformSync
  }

  case class equalToRepr[L](expected: Repr) extends Matcher[\/[L, Repr]] {
    def apply[S <: \/[L, Repr]](s: Expectable[S]) = {
      val expectedTree = RenderTreeT[Fix].render(expected).right[L]
      val actualTree = s.value.map(repr => RenderTreeT[Fix].render(repr))
      val diff = (expectedTree ⊛ actualTree) {
        case (e, a) => (e diff a).shows
      }

      result(expected.right[L] == s.value,
             "\ntrees are equal:\n" + diff,
             "\ntrees are not equal:\n" + diff,
             s,
             NoDetails)
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

  def beRepr(expected: SqlExpr[Fix[SqlExpr]]) =
    equalToRepr[FileSystemError](Fix(expected))

  import SqlExpr._
  import SqlExpr.Select._

  "Shifted read" should {
    type SR[A] = Const[ShiftedRead[AFile], A]

    "build plan for column wildcard" in {
      plan(sqlE"select * from foo") must
        beRepr({
          val all = Selection[Fix[SqlExpr]](Fix(AllCols()), alias = None)
          Select(all, From(Fix(Select(all, From(Fix(Table("db.foo")), alias = None), filter = None)),
            alias = Id("_0").some), filter = None)
        })
    }

    def expectShiftedReadRepr(forIdStatus: IdStatus, expectedRepr: SqlExpr[Fix[SqlExpr]]) = {
      val path: AFile = rootDir </> dir("db") </> file("foo")

      val qs: Fix[SR] = Fix(Inject[SR, SR].inj(Const(ShiftedRead(path, forIdStatus))))
      val planner = Planner.constShiftedReadFilePlanner[Fix, Task]
      val repr = qs.cataM(planner.plan).map(_.convertTo[Repr]).unsafePerformSync

      repr.right[FileSystemError] must
        beRepr(expectedRepr)
    }

    "build plan including ids" in {
      expectShiftedReadRepr(forIdStatus = IncludeId,
        expectedRepr = {
          val allWithIds = Selection[Fix[SqlExpr]](Fix(WithIds(Fix(AllCols()))), alias = None)
          Select(allWithIds, From(Fix(Table("db.foo")), alias = None), filter = None)
        })
    }

    "build plan only for ids" in {
      expectShiftedReadRepr(forIdStatus = IdOnly,
        expectedRepr = {
          val rowIds = Selection[Fix[SqlExpr]](Fix(RowIds()), alias = None)
          Select(rowIds, From(Fix(Table("db.foo")), alias = None), filter = None)
        })
    }

    "build plan only for excluded ids" in {
      expectShiftedReadRepr(forIdStatus = ExcludeId,
        expectedRepr = {
          val all = Selection[Fix[SqlExpr]](Fix(AllCols()), alias = None)
          Select(all, From(Fix(Table("db.foo")), alias = None), filter = None)
        })
    }
  }
}
