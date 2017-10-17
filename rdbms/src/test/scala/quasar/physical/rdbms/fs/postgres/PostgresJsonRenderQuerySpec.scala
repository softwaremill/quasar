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
import quasar.Qspec
import quasar.physical.rdbms.fs.postgres.planner.PostgresJsonRenderQuery
import quasar.physical.rdbms.planner.sql.SqlExprSupport
import quasar.sql._

import eu.timepit.refined.auto._

class PostgresJsonRenderQuerySpec extends Qspec with SqlExprSupport {

  "PostgresJsonRenderQuery" should {
    "render select with single field projection" in {
      plan(sqlE"select user from foo")
        .flatMap(repr => PostgresJsonRenderQuery.asString(repr)) must
        beRightDisjunction("(select data::json#>'{\"user\"}' from (select * from db.foo ) as _0 )")
    }

    "render select with a single embedded field projection" in {
      plan(sqlE"select user.name from foo")
        .flatMap(repr => PostgresJsonRenderQuery.asString(repr)) must
        beRightDisjunction("(select data::json#>'{\"user\",\"name\"}' from (select * from db.foo ) as _0 )")
    }

    "render select with an embedded field projection + array ref" in {
      plan(sqlE"select user[1].name from foo")
        .flatMap(repr => PostgresJsonRenderQuery.asString(repr)) must
        beRightDisjunction("(select data::json#>'{\"user\",1,\"name\"}' from (select * from db.foo ) as _0 )")
    }


  }
}
