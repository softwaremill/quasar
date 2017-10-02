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

package quasar.physical.rdbms.fs.postgres.planner

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.planner.SqlExprBuilder
import quasar.qscript.{ExcludeId, IdOnly, IncludeId, ShiftedRead}
import doobie.syntax.string._
import doobie.util.fragment.Fragment

import scalaz._
import Scalaz._

object PostgresSqlExprBuilder extends SqlExprBuilder {

  def selectAll(semantics: ShiftedRead[AFile]): Fragment = {
    val tablePath = TablePath.create(semantics.path)

    val resultExpr = semantics.idStatus match  {
      case IdOnly => fr"row_number() over()"
      case ExcludeId => fr"data"
      case IncludeId => fr"row_number() over(), data"
    }
    fr"select" ++ resultExpr ++ fr"from" ++ Fragment.const0(tablePath.shows)

  }

}
