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

package quasar.physical.rdbms.planner.sql

import slamdata.Predef._

import matryoshka.BirecursiveT
import matryoshka.implicits._
import scalaz._

object Indirections {

  sealed trait IndirectionType
  final case object Field extends IndirectionType
  final case object InnerField extends IndirectionType

  sealed trait Indirection

  final case class Branch(m: String => (IndirectionType, Indirection), debug: String) extends Indirection

  implicit val metaShow: Show[Indirection] = {
    Show.shows[Indirection] {
      case Branch(_, debug) => debug
    }
  }

  val InnerRef: Indirection = Branch((_: String) => (InnerField, InnerRef), "InnerRef")
  val Default = Branch((_: String) => (Field, InnerRef), "Default")

  import SqlExpr._

  @tailrec
  def deriveIndirection[T[_[_]]: BirecursiveT](expr: T[SqlExpr]): Indirection = {
    expr.project match {
      case SqlExpr.Id(_, m) =>
        m
      case ExprPair(_, _, m) =>
        m
      case ExprWithAlias(v, _) =>
        deriveIndirection(v)
      case Select(Selection(_, _, m), _, _, _, _, _) =>
        m
      case _ =>
        Default
    }
  }
}


