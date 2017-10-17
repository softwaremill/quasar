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
import quasar.{Data => QData}
import quasar.contrib.pathy._
import quasar.fs.FileSystemError
import quasar.Qspec
import quasar.physical.rdbms.fs.postgres.Postgres
import quasar.physical.rdbms.fs.postgres.planner.PostgresJsonRenderQuery
import quasar.physical.rdbms.model.Repr
import quasar.physical.rdbms.planner.Planner
import quasar.qscript._
import quasar.sql._

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import org.specs2.execute.NoDetails
import org.specs2.matcher._
import pathy.Path._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

class PlannerSpec extends Qspec with SqlExprSupport {

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

  def beRepr(expected: SqlExpr[Fix[SqlExpr]]) =
    equalToRepr[FileSystemError](Fix(expected))

  import SqlExpr._
  import SqlExpr.Select._

  def data(v: String): Fix[SqlExpr] =
    Fix(Data[Fix[SqlExpr]](QData.Str(v)))

  def length(v: Fix[SqlExpr]): Fix[SqlExpr] =
    Fix(Length[Fix[SqlExpr]](v))

  def selection(
      v: Fix[SqlExpr],
      alias: Option[SqlExpr.Id[Fix[SqlExpr]]] = None): Selection[Fix[SqlExpr]] =
    Selection[Fix[SqlExpr]](v, alias)

  def * : Fix[SqlExpr] = Fix(AllCols())

  def exprs(e1: Fix[SqlExpr], e2: Fix[SqlExpr]): Fix[SqlExpr] = Fix(ExprPair[Fix[SqlExpr]](e1, e2))

  def alias(e: Fix[SqlExpr], a: String): Fix[SqlExpr] = Fix(ExprWithAlias[Fix[SqlExpr]](e, Id[Fix[SqlExpr]](a)))

  def alias(a: String): Fix[SqlExpr] = alias(data(a), a)

  def fromTable(
      name: String,
      alias: Option[SqlExpr.Id[Fix[SqlExpr]]] = None): From[Fix[SqlExpr]] =
    From[Fix[SqlExpr]](Fix(Table(name)), alias)

  def select[T](selection: Selection[T],
                from: From[T],
                filter: Option[Filter[T]] = None) =
    Select(selection, from, filter)

  "Shifted read" should {
    type SR[A] = Const[ShiftedRead[AFile], A]

    "build plan for column wildcard" in {
      plan(sqlE"select * from foo") must
        beRepr({
          select(
            selection(*),
            From(Fix(Select(selection(*), fromTable("db.foo"), filter = None)),
                 alias = Id("_0").some))
        })
    }

    def expectShiftedReadRepr(forIdStatus: IdStatus,
                              expectedRepr: SqlExpr[Fix[SqlExpr]]) = {
      val path: AFile = rootDir </> dir("db") </> file("foo")

      val qs: Fix[SR] =
        Fix(Inject[SR, SR].inj(Const(ShiftedRead(path, forIdStatus))))
      val planner = Planner.constShiftedReadFilePlanner[Fix, Task]
      val repr = qs.cataM(planner.plan).map(_.convertTo[Repr]).unsafePerformSync

      repr.right[FileSystemError] must
        beRepr(expectedRepr)
    }

    "build plan including ids" in {
      expectShiftedReadRepr(forIdStatus = IncludeId, expectedRepr = {
        select(selection(Fix(WithIds(*))), fromTable("db.foo"))
      })
    }

    "build plan only for ids" in {
      expectShiftedReadRepr(forIdStatus = IdOnly, expectedRepr = {
        select(selection(Fix(RowIds())), fromTable("db.foo"))
      })
    }

    "build plan only for excluded ids" in {
      expectShiftedReadRepr(forIdStatus = ExcludeId, expectedRepr = {
        select(selection(*), fromTable("db.foo"))
      })
    }
  }

  "MapFuncCore" should {

    "build length()" in {
      plan(sqlE"select length(name) from foo") must
        beRepr({
          select(selection(length(data("name"))),
                 From(Fix(select(selection(*), fromTable("db.foo"))),
                      alias = Id("_0").some))
        })
    }

    "build projection for a single field" in {
      plan(sqlE"select name from foo") must
        beRepr({
          select(selection(data("name")),
            From(Fix(select(selection(*), fromTable("db.foo"))),
              alias = Id("_0").some))
        })
    }

    "build projection for multiple fields" in {
      plan(sqlE"select name, surname, street from foo") must
        beRepr({
          select(selection(exprs(exprs(alias("name"), alias("surname")), alias("street"))),
            From(Fix(select(selection(*), fromTable("db.foo"))), alias = Id("_0").some))
        })
    }

    "build length with additional fields and an alias" in {
      plan(sqlE"select name, length(surname) as len from foo") must
        beRepr({
          select(selection(exprs(alias("name"), alias(length(data("surname")), "len"))),
            From(Fix(select(selection(*), fromTable("db.foo"))), alias = Id("_0").some))
        })
    }


    "util" in {
      val qsOrErr = (compileSqlToLP[EitherWriter](sqlE"select name, surname from foo2") >>= (lp =>
        Postgres.lpToQScript(lp, listContents))).run.run._2

      qsOrErr.map { qs =>
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>> QSCRIPT >>>>>>>>>>>>>>>>>>>")
        println(qs)
        val qsTree = RenderTreeT[Fix].render(qs).shows
        println(qsTree)
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>> REPR >>>>>>>>>>>>>>>>>>>")
        val repr = qsToRepr(qs)
        val tree = RenderTreeT[Fix].render(repr).shows
        println(tree)
        println(">>>>>>>>>>>>>>>>>>>>>>>>>>> SQL >>>>>>>>>>>>>>>>>>>")
        val query = PostgresJsonRenderQuery.asString(repr)
        println(query)

      }
      1.must_===(1)
    }
  }
}
