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
import quasar.{Qspec, TreeMatchers}
import quasar.Planner.PlannerError
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.construction
import quasar.qscript.MapFuncsCore

import matryoshka._
import matryoshka.data._
import matryoshka.data.free._
import pathy.Path
import scalaz.{\/, \/-, EitherT, ICons, INil, Need, NonEmptyList => NEL, StateT}
import scalaz.Scalaz._

object ExtractFreeMapSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QScriptUniform.{DTrans, Retain, Rotation}
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]
  type QSU[A] = QScriptUniform[A]

  val ejs = Fixed[Fix[EJson]]
  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]

  def projectStrKey(key: String): FreeMap = func.ProjectKeyS(func.Hole, key)

  def makeMap(left: String, right: String): JoinFunc =
    func.StaticMapS(
      left -> func.LeftSide,
      right -> func.RightSide)

  val orders: AFile = Path.rootDir </> Path.dir("client") </> Path.file("orders")

  def extractFM(graph: QSUGraph) = ExtractFreeMap[Fix, F](graph)

  "extracting mappable region" should {

    "convert mappable filter predicate" >> {
      val predicate = projectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.map(qsu.read(orders), predicate)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(QSFilter(Read(`orders`), fm)) => fm must_= predicate
      }
    }

    "convert transposed filter predicate" >> {
      val predicate = projectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.transpose(qsu.map(qsu.read(orders), predicate), Retain.Values, Rotation.ShiftMap)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSFilter(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              filterPredicate),
            valueAccess)) =>

          fm must beTreeEqual(predicate)

          filterPredicate must beTreeEqual(projectStrKey("filter_predicate_0"))

          valueAccess must beTreeEqual(projectStrKey("filter_source"))

          autojoinCondition must beTreeEqual(makeMap("filter_source", "filter_predicate_0"))
      }
    }

    "greedily convert mappable function applied to transposed filter predicate" >> {
      val field = projectStrKey("foo")

      val predicate =
        func.Gt(func.Hole, func.Constant(ejs.int(42)))

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.autojoin2((
            qsu.transpose(qsu.map(qsu.read(orders), field), Retain.Values, Rotation.ShiftMap),
            qsu.cint(42),
            _(MapFuncsCore.Gt(_, _))))))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSFilter(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              filterPredicate),
            valueAccess)) =>

          fm must beTreeEqual(field)

          filterPredicate must beTreeEqual(predicate >> projectStrKey("filter_predicate_0"))

          valueAccess must beTreeEqual(projectStrKey("filter_source"))

          autojoinCondition must beTreeEqual(makeMap("filter_source", "filter_predicate_0"))
      }
    }

    "convert mappable group key" >> {
      val key = projectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.map(qsu.read(orders), key)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(DimEdit(Read(`orders`), DTrans.Group(fm))) => fm must_= key
      }
    }

    "convert transposed group key" >> {
      val key = projectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.transpose(qsu.map(qsu.read(orders), key), Retain.Values, Rotation.ShiftMap)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            DimEdit(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              DTrans.Group(groupKey)),
            valueAccess)) =>

          fm must beTreeEqual(key)

          groupKey must beTreeEqual(projectStrKey("group_key_0"))

          valueAccess must beTreeEqual(projectStrKey("group_source"))

          autojoinCondition must beTreeEqual(makeMap("group_source", "group_key_0"))
      }
    }

    "greedily convert mappable function applied to transposed group key" >> {
      val key = projectStrKey("foo")

      val f =
        func.Add(func.Modulo(func.Hole, func.Constant(ejs.int(13))), func.Constant(ejs.int(1)))

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.map(
            qsu.transpose(qsu.map(qsu.read(orders), key), Retain.Values, Rotation.ShiftMap),
            f)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            DimEdit(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              DTrans.Group(groupKey)),
            valueAccess)) =>

          fm must beTreeEqual(key)

          groupKey must beTreeEqual(f >> projectStrKey("group_key_0"))

          valueAccess must beTreeEqual(projectStrKey("group_source"))

          autojoinCondition must beTreeEqual(makeMap("group_source", "group_key_0"))
      }
    }

    "convert mappable sort keys" >> {
      val key1 = projectStrKey("foo")
      val key2 = projectStrKey("foo")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.map(qsu.read(orders), key2) -> SortDir.Descending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(QSSort(
            Read(`orders`),
            Nil,
            NEL((fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), INil())))) =>
          fm1 must_= key1
          fm2 must_= key2
      }
    }

    "convert transposed sort key and mappable sort key" >> {
      val key1 = projectStrKey("foo")
      val key2 = projectStrKey("bar")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.transpose(qsu.map(qsu.read(orders), key2), Retain.Values, Rotation.ShiftMap) -> SortDir.Descending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSSort(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              Nil,
              NEL((fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), INil()))),
            valueAccess)) => {

          fm must beTreeEqual(key2)

          fm1 must beTreeEqual(key1 >> projectStrKey("sort_source"))

          fm2 must beTreeEqual(projectStrKey("sort_key_0"))

          valueAccess must beTreeEqual(projectStrKey("sort_source"))

          autojoinCondition must beTreeEqual(makeMap("sort_source", "sort_key_0"))
        }
      }
    }

    "convert two transposed sort keys and two mappable sort keys" >> {
      val key1 = projectStrKey("key1")
      val key2 = projectStrKey("key2")
      val key3 = projectStrKey("key3")
      val key4 = projectStrKey("key4")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.transpose(qsu.map(qsu.read(orders), key2), Retain.Values, Rotation.ShiftMap) -> SortDir.Descending,
            qsu.map(qsu.read(orders), key3) -> SortDir.Descending,
            qsu.transpose(qsu.map(qsu.read(orders), key4), Retain.Identities, Rotation.ShiftArray) -> SortDir.Ascending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSSort(
              AutoJoin2(
                AutoJoin2(
                  Read(`orders`),
                  Transpose(Map(Read(`orders`), innerFM), Retain.Values, Rotation.ShiftMap),
                  innerAutojoinCondition),
                Transpose(Map(Read(`orders`), outerFM), Retain.Identities, Rotation.ShiftArray),
                outerAutojoinCondition),
              Nil,
              NEL(
                (fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), ICons((fm3, SortDir.Descending), ICons((fm4, SortDir.Ascending), INil()))))),
            valueAccess)) => {

          innerFM must beTreeEqual(key2)

          outerFM must beTreeEqual(key4)

          fm1 must beTreeEqual(key1 >> projectStrKey("sort_source"))

          fm2 must beTreeEqual(projectStrKey("sort_key_0"))

          fm3 must beTreeEqual(key3 >> projectStrKey("sort_source"))

          fm4 must beTreeEqual(projectStrKey("sort_key_1"))

          valueAccess must beTreeEqual(projectStrKey("sort_source"))

          innerAutojoinCondition must beTreeEqual(makeMap("sort_source", "sort_key_0"))

          outerAutojoinCondition must beTreeEqual(func.ConcatMaps(
            func.LeftSide,
            func.MakeMap(func.Constant(ejs.str("sort_key_1")), func.RightSide)))
        }
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
