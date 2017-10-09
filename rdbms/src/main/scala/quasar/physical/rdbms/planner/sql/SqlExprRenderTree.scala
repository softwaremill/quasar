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
import quasar.{NonTerminal, RenderTree, RenderedTree, Terminal}

import matryoshka.Delay
import scalaz._, Scalaz._

trait SqlExprRenderTree {

  import SqlExpr._

  implicit val renderTree: Delay[RenderTree, SqlExpr] =
    new Delay[RenderTree, SqlExpr] {
      def apply[A](r: RenderTree[A]): RenderTree[SqlExpr[A]] = {

        def nonTerminal(typ: String, c: A*): RenderedTree =
          NonTerminal(typ :: Nil, none, c.toList.map(r.render))

        RenderTree.make {
          case Id(v) =>
            Terminal("Id" :: Nil, v.some)
          case Table(v) =>
            nonTerminal("Table", v)
          case Select(_, _, _) =>
            Terminal("SELECT(TODO)" :: Nil, Some("TODO"))

        }
      }
    }
}
