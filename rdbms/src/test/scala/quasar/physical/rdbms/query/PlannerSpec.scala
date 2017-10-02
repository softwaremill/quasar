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

package quasar.physical.rdbms.query

import quasar.contrib.pathy.AFile
import quasar.fp.free._
import quasar.qscript._
import doobie.util.fragment.Fragment
import matryoshka.AlgebraM
import org.specs2.scalaz.DisjunctionMatchers
import pathy.Path.{dir, file, rootDir}
import quasar.Planner.PlannerError
import quasar.physical.rdbms.fs.postgres.planner.PostgresSqlExprBuilder
import quasar.physical.rdbms.planner.Planner

import scalaz._
import scalaz.concurrent.Task

class PlannerSpec
    extends quasar.Qspec
    with QScriptHelpers
    with DisjunctionMatchers {

  sequential

  type RdbmsState[S[_], A] = EitherT[Free[S, ?], PlannerError, A]
  type F[A] = RdbmsState[Task, A]
  val sr = Planner.shiftedread[F]
  val exprBuilder = PostgresSqlExprBuilder

  private def runTest[A, S[_]](f: => Free[S, A])(implicit S: S :<: Task): A = {
    f.foldMap(injectNT[S, Task]).unsafePerformSync
  }

  "Planner" should {
    "shiftedReadFile with ExcludeId" in {
      runTest {
        val compile: AlgebraM[F, Const[ShiftedRead[AFile], ?], Fragment] = sr.plan(exprBuilder)

        val afile: AFile = rootDir </> dir("irrelevant") </> dir("test") </> file("targetFile17")

        val program = compile(Const(ShiftedRead(afile, ExcludeId)))
        program.run.map(result => result must beRightDisjunction.like {
          case queryFragment =>
            queryFragment.toString must_=== """Fragment("select data from irrelevant__c_test.targetfile17")"""
        })
      }
    }

    "shiftedReadFile with IncludeId" in {
      runTest {
        val compile: AlgebraM[F, Const[ShiftedRead[AFile], ?], Fragment] = sr.plan(exprBuilder)

        val afile: AFile = rootDir </> dir("irrelevant") </> dir("test2") </> file("targetFile18")

        val program = compile(Const(ShiftedRead(afile, IncludeId)))
        program.run.map(result => result must beRightDisjunction.like {
          case queryFragment =>
            queryFragment.toString must_=== """Fragment("select row_number() over(), data from irrelevant__c_test2.targetfile18")"""
        })
      }
    }

    "shiftedReadFile with IdOnly" in {
      runTest {
        val compile: AlgebraM[F, Const[ShiftedRead[AFile], ?], Fragment] = sr.plan(exprBuilder)

        val afile: AFile = rootDir </> dir("irrelevant") </> dir("test3") </> file("targetFile19")

        val program = compile(Const(ShiftedRead(afile, IdOnly)))
        program.run.map(result => result must beRightDisjunction.like {
          case queryFragment =>
            queryFragment.toString must_=== """Fragment("select row_number() over() from irrelevant__c_test3.targetfile19")"""
        })
      }
    }

  }
}
