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

package quasar.qscript.qsu

import slamdata.Predef._

import quasar.NameGenerator
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.fp.symbolOrder
import quasar.qscript.{construction, JoinSide, LeftSide, RightSide}
import quasar.sql.JoinDir

import matryoshka.BirecursiveT
import scalaz.{Monad, NonEmptyList, Scalaz, StateT}, Scalaz._

/** Extracts `MapFunc` expressions from operations by requiring an argument
  * to be a function of one or more sibling arguments and creating an
  * autojoin if not.
  */
final class ExtractFreeMap[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QScriptUniform._
  import QSUGraph.Extractors

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](graph: QSUGraph)
      : F[QSUGraph] = {
    type G[A] = StateT[F, RevIdx, A]
    graph.rewriteM[G](extract[G]).eval(graph.generateRevIndex)
  }

  ////

  private type QSU[A] = QScriptUniform[A]

  private val func = construction.Func[T]

  private def extract[F[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM]
      : PartialFunction[QSUGraph, F[QSUGraph]] = {

    case graph @ Extractors.GroupBy(src, key) =>
      unifyShapePreserving[F](graph, src.root, NonEmptyList(key.root))("group_source", "group_key") {
        case (sym, fms) => DimEdit(sym, DTrans.Group(fms.head))
      }

    case graph @ Extractors.LPFilter(src, predicate) =>
      unifyShapePreserving[F](graph, src.root, NonEmptyList(predicate.root))("filter_source", "filter_predicate") {
        case (sym, fms) => QSFilter(sym, fms.head)
      }

    case graph @ Extractors.LPJoin(left, right, cond, jtype, lref, rref) =>
      val combiner: JoinFunc =
        func.StaticMapS(
          JoinDir.Left.name -> func.LeftSide,
          JoinDir.Right.name -> func.RightSide)

      MappableRegion.funcOf(replaceRefs(graph, lref, rref), graph refocus cond.root)
        .map(jf => ThetaJoin(left.root, right.root, jf, jtype, combiner)) match {
          case Some(qs) =>
            graph.overwriteAtRoot(qs).point[F]
          case None =>
            PlannerErrorME[F].raiseError[QSUGraph](
              InternalError(s"Invalid join condition, $cond, must be a mappable function of $left and $right.", None))
        }

    case graph @ Extractors.LPSort(src, keys) =>
      unifyShapePreserving[F](graph, src.root, keys map (_._1.root))("sort_source", "sort_key") {
        case (sym, fms) => QSSort(sym, Nil, fms fzip keys.seconds)
      }
  }

  private def replaceRefs(g: QSUGraph, l: Symbol, r: Symbol)
      : Symbol => Option[JoinSide] =
    s => g.vertices.get(s) collect {
      case JoinSideRef(`l`) => LeftSide
      case JoinSideRef(`r`) => RightSide
    }

  private def unifyShapePreserving[F[_]: Monad: NameGenerator: RevIdxM](
      graph: QSUGraph,
      source: Symbol,
      targets: NonEmptyList[Symbol])(
      sourceName: String,
      targetPrefix: String)(
      buildNode: (Symbol, NonEmptyList[FreeMap]) => QScriptUniform[Symbol]): F[QSUGraph] =
    UnifyTargets[T, F](withName[F](_))(graph, source, targets)(sourceName, targetPrefix) flatMap {
      case (newSrc, original, targetExprs) =>
        val node = buildNode(newSrc.root, targetExprs)

        if (newSrc.root === source)
          graph.overwriteAtRoot(node).point[F]
        else
          withName[F](node) map { inter =>
            graph.overwriteAtRoot(Map(inter.root, original)) :++ inter :++ newSrc
          }
    }

  private def withName[F[_]: Monad: NameGenerator: RevIdxM](node: QScriptUniform[Symbol]): F[QSUGraph] =
    QSUGraph.withName[T, F]("efm")(node)
}

object ExtractFreeMap {
  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    taggedInternalError("ExtractFreeMap", new ExtractFreeMap[T].apply[F](graph))
}
