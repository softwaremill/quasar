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

import scalaz._, Scalaz._
import matryoshka._

trait SqlExprInstances extends SqlExprTraverse with SqlExprRenderTree with SqlExprDelayEqual

trait SqlExprDelayEqual {

  implicit def delayEqSqlExpr = new Delay[Equal, SqlExpr] {
    def apply[A](fa: Equal[A]): Equal[SqlExpr[A]] = Equal.equalA
  }
}

trait SqlExprTraverse {
  import SqlExpr._, Select._, Case._

  implicit val traverse: Traverse[SqlExpr] = new Traverse[SqlExpr] {
    def traverseImpl[G[_], A, B](
        fa: SqlExpr[A]
    )(
        f: A => G[B]
    )(
        implicit G: Applicative[G]
    ): G[SqlExpr[B]] = fa match {
      case Unreferenced()      => G.point(Unreferenced())
      case Null()              => G.point(Null())
      case Obj(m)              => m.traverse(_.bitraverse(f, f)) ∘ (l => Obj(l))
      case Constant(d)         => G.point(Constant(d))
      case Id(str, m)             => G.point(Id(str, m))
      case RegexMatches(a1, a2, i) => (f(a1) ⊛ f(a2))(RegexMatches(_, _, i))
      case ExprWithAlias(e, a) => (f(e) ⊛ G.point(a))(ExprWithAlias.apply)
      case ExprPair(e1, e2,m )    => (f(e1) ⊛ f(e2) ⊛ G.point(m))(ExprPair.apply)
      case ConcatStr(a1, a2)   => (f(a1) ⊛ f(a2))(ConcatStr(_, _))
      case Avg(a1)             => f(a1) ∘ (Avg(_))
      case Count(a1)           => f(a1) ∘ (Count(_))
      case Max(a1)             => f(a1) ∘ (Max(_))
      case Min(a1)             => f(a1) ∘ (Min(_))
      case Sum(a1)             => f(a1) ∘ (Sum(_))
      case Length(a1)          => f(a1) ∘ (Length(_))
      case DeleteKey(a1, a2)   => (f(a1) ⊛ f(a2))(DeleteKey(_, _))
      case Distinct(a1)        => f(a1) ∘ (Distinct(_))
      case Refs(srcs, m)       =>  (srcs.traverse(f) ⊛ G.point(m))(Refs.apply)
      case Table(name)         => G.point(Table(name))
      case IsNotNull(v)        => f(v) ∘ IsNotNull.apply
      case IfNull(v)           => v.traverse(f) ∘ (IfNull(_))
      case RowIds()            => G.point(RowIds())
      case AllCols()           =>  G.point(AllCols())
      case NumericOp(op, left, right) => (f(left) ⊛ f(right))(NumericOp(op, _, _))
      case Mod(a1, a2)         => (f(a1) ⊛ f(a2))(Mod.apply)
      case Pow(a1, a2)         => (f(a1) ⊛ f(a2))(Pow.apply)
      case And(a1, a2)         => (f(a1) ⊛ f(a2))(And(_, _))
      case Not(v)              => f(v) ∘ Not.apply
      case Eq(a1, a2)          => (f(a1) ⊛ f(a2))(Eq(_, _))
      case Neq(a1, a2)         => (f(a1) ⊛ f(a2))(Neq(_, _))
      case Lt(a1, a2)          => (f(a1) ⊛ f(a2))(Lt(_, _))
      case Lte(a1, a2)         => (f(a1) ⊛ f(a2))(Lte(_, _))
      case Gt(a1, a2)          => (f(a1) ⊛ f(a2))(Gt(_, _))
      case Gte(a1, a2)         => (f(a1) ⊛ f(a2))(Gte(_, _))
      case Or(a1, a2)          => (f(a1) ⊛ f(a2))(Or(_, _))
      case Neg(v)              => f(v) ∘ Neg.apply
      case WithIds(v)          => f(v) ∘ WithIds.apply

      case Select(selection, from, joinOpt, filterOpt, groupBy, order) =>
        val newOrder = order.traverse(o => f(o.v).map(newV => OrderBy(newV, o.sortDir)))
        val sel = f(selection.v) ∘ (i => Selection(i, selection.alias ∘ (a => Id[B](a.v, a.meta)), selection.meta))

        val join = joinOpt.traverse(j => (f(j.v) ⊛ j.keys.traverse { case (a, b) => (f(a) ⊛ f(b))(scala.Tuple2.apply)}) {
          case (v, ks) => Join(v, ks, j.jType, Id[B](j.alias.v, j.alias.meta))
        })

        val alias = f(from.v).map(b => From(b, Id[B](from.alias.v, from.alias.meta)))

        (sel ⊛
          alias ⊛
          join ⊛
          filterOpt.traverse(i => f(i.v) ∘ Filter.apply) ⊛
          groupBy.traverse(i => i.v.traverse(f) ∘ GroupBy.apply) ⊛
          newOrder)(
          Select(_, _, _, _, _, _)
        )
      case Union(left, right) => (f(left) ⊛ f(right))(Union.apply)
      case Case(wt, Else(e)) =>
        (wt.traverse { case WhenThen(w, t) => (f(w) ⊛ f(t))(WhenThen(_, _)) } ⊛
          f(e)
          )((wt, e) =>
          Case(wt, Else(e))
        )

      case TypeOf(e) => f(e) ∘ TypeOf.apply
      case Coercion(t, e) => f(e) ∘ (Coercion(t, _))
      case ToArray(v) => f(v) ∘ ToArray.apply
      case UnaryFunction(t, e) => f(e) ∘ (UnaryFunction(t, _))
      case BinaryFunction(t, a1, a2) => (f(a1) ⊛ f(a2))(BinaryFunction(t, _, _))
      case TernaryFunction(t, a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(TernaryFunction(t, _, _, _))
      case Limit(from, count) => (f(from) ⊛ f(count))(Limit.apply)
      case Offset(from, count) => (f(from) ⊛ f(count))(Offset.apply)
      case ArrayUnwind(u) => f(u) ∘ ArrayUnwind.apply
      case Time(a1)       => f(a1) ∘ Time.apply
      case Timestamp(a1)  => f(a1) ∘ Timestamp.apply
      case DatePart(part, e) => (f(part) ⊛ f(e))(DatePart(_, _))
    }
  }
}
