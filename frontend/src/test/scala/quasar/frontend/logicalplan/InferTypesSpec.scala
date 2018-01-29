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

package quasar.frontend.logicalplan

import quasar.Type

import quasar.fp.ski.κ

import matryoshka.data.Fix
import org.specs2.execute._
import scalaz.Cofree

class InferTypesSpec extends quasar.Qspec {

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  def infer(lp: Fix[LogicalPlan])(eval: Cofree[LogicalPlan, Type] => ResultLike): Result = 
    lpf.inferTypes(Type.Top, lp).fold(κ(failure), eval(_).toResult)
  
  "inferTypes" should  {

    "be Top for Free" in {
      infer(lpf.free('sym)) { cf =>
        cf.head must_== Type.Top
        cf.tail must_== lpf.free('sym).unFix
      }
    }

    "be Top for Let(Free, Free)" in {
      infer(lpf.let('sym, lpf.free('sym), lpf.free('sym))) { cf =>
        cf.head must_== Type.Top
        val (sym, form, in) = let.getOption(cf.tail).get
        sym must_== 'sym
        form.head must_== Type.Top
        free.getOption(form.tail).get must_== 'sym
        free.getOption(in.tail).get must_== 'sym
      }
    }
  }
}
