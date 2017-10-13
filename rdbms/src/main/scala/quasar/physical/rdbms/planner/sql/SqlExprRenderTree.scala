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
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._

import matryoshka.Delay
import scalaz._
import Scalaz._

trait SqlExprRenderTree {

  import SqlExpr._

  implicit val renderTree: Delay[RenderTree, SqlExpr] =
    new Delay[RenderTree, SqlExpr] {
      def apply[A](r: RenderTree[A]): RenderTree[SqlExpr[A]] = {

        def renderFields(fields: Fields[A]): String =
          fields match {
            case AllCols()                => "*"
            case RowIds()                 => "(row ids)"
            case WithIds(AllCols())       => "(row ids, *)"
            case WithIds(SomeCols(names)) => names.mkString(",")
            case SomeCols(names)          => names.mkString(",")
          }

        def nonTerminal(typ: String, c: A*): RenderedTree =
          NonTerminal(typ :: Nil, none, c.toList ∘ r.render)

        RenderTree.make {
          case Id(v) =>
            Terminal("Id" :: Nil, v.some)
          case Table(v) =>
            Terminal("Table" :: Nil, v.some)
          case Select(fields, from, filter) =>
            def nt(tpe: String, label: Option[String], child: A) =
              NonTerminal(tpe :: Nil, label, List(r.render(child)))

            NonTerminal(
              "Select" :: Nil,
              none,
              Terminal("fields" :: Nil, renderFields(fields).some) ::
                nt("from", from.alias ∘ (_.v), from.v) ::
                (filter ∘ (f => nt("filter", none, f.v))).toList
            )
        }
      }
    }
}
