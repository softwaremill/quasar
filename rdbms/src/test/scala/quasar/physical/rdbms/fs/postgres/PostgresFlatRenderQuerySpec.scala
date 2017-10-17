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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar._
import quasar.sql._
import quasar.physical.rdbms.fs.postgres.planner.PostgresFlatRenderQuery
import quasar.physical.rdbms.planner.sql.SqlExprSupport

class PostgresFlatRenderQuerySpec extends Qspec with SqlExprSupport {

  "PostgresFlatRenderQuery" should {
    "render select with single field projection" in {
      plan(sqlE"select user from foo")
        .flatMap(repr => PostgresFlatRenderQuery.asString(repr)) must
        beRightDisjunction("(select user from (select * from db.foo ) as _0 )")
    }
  }
}
