/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir

import quasar.precog.common._
import quasar.precog.util.Identifier
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.precog.TestSupport._

object DAGSpecs extends Specification with DAG with FNDummyModule {
  import dag._

  type Lib = RandomLibrary
  val library = new RandomLibrary {}

  "mapDown" should {
    "rewrite a AbsoluteLoad shared across Split branches to the same object" in {
      val load = dag.AbsoluteLoad(Const(CString("/clicks")))

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, load, UnfixedSolution(0, load)),
        SplitParam(0, id), id)

      val result = input.mapDown { recurse => {
        case graph @ dag.AbsoluteLoad(Const(CString(path)), tpe) =>
          dag.AbsoluteLoad(Const(CString("/foo" + path)), tpe)
      }}

      result must beLike {
        case dag.Split(dag.Group(_, load1, UnfixedSolution(_, load2)), _, _) =>
          load1 must be(load2)
      }
    }
  }

  "foldDown" should {
    "look within a Split branch" in {
      val load = dag.AbsoluteLoad(Const(CString("/clicks")))

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, load, UnfixedSolution(0, load)),
        SplitParam(0, id), id)

      import scalaz.std.anyVal._
      val result = input.foldDown[Int](true) {
        case _: AbsoluteLoad => 1
      }

      result mustEqual 2
    }
  }
}
