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

package quasar

import slamdata.Predef.None
import quasar.frontend.logicalplan.LogicalPlan
import quasar.sql.Sql

import matryoshka.data.Fix
import pathy.Path.rootDir
import eu.timepit.refined.auto._

import scalaz.Scalaz._

object Fixture {
  def unsafeToLP(q: Fix[Sql], vars: Variables = Variables.empty): Fix[LogicalPlan] =
    quasar.queryPlan(q, vars, rootDir, 0L, None).run.value.valueOr(
      err => scala.sys.error("Unexpected error compiling sql to LogicalPlan: " + err.shows))
}