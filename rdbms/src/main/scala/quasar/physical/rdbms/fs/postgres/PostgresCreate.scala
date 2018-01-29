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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.physical.rdbms.fs.RdbmsCreate
import quasar.physical.rdbms.common._
import quasar.physical.rdbms.model._

import doobie.syntax.string._
import doobie.free.connection.ConnectionIO
import doobie.util.fragment.Fragment
import scalaz._
import Scalaz._

trait PostgresCreate extends RdbmsCreate {

  private def createSchemas(schemas: List[Schema]): ConnectionIO[Unit] = {
    schemas.traverse( s =>
      if (s.isRoot)
        ().point[ConnectionIO]
      else
        fr"LOCK TABLE pg_catalog.pg_namespace".update.run *>
          (fr"CREATE SCHEMA IF NOT EXISTS" ++ Fragment.const(s""""${s.shows}"""")).update.run.void).void
  }

  override def ensureSchemaParents(schema: Schema): ConnectionIO[Unit] = {
    createSchemas(schema.parents)
  }

  override def createSchema(schema: Schema): ConnectionIO[Unit] = {
    createSchemas(schema :: schema.parents)
  }

  override def createTable(tablePath: TablePath, model: TableModel): ConnectionIO[Unit] =
    (createSchema(tablePath.schema) *> (fr"CREATE TABLE IF NOT EXISTS" ++ Fragment
      .const(tablePath.shows) ++ fr"(" ++ modelToColumns(model) ++ fr")").update.run).void

  def modelToColumns(model: TableModel): Fragment = {
    model match {
      case JsonTable => Fragment.const("data jsonb NOT NULL")
      case ColumnarTable(cols) =>
        cols.toList.map(c => Fragment.const(s""""${c.name}" ${c.tpe.mapToStringName}""")).intercalate(fr",")
    }
  }

  override def alterTable(tablePath: TablePath, cols: Set[AlterColumn]): ConnectionIO[Unit] = {
    if (cols.isEmpty)
      ().point[ConnectionIO]
    else {
        val dbPath = Fragment.const(tablePath.shows)
        cols.foldMap {
          case AddColumn(name, tpe) =>
            (fr"ALTER TABLE" ++ dbPath ++
            Fragment.const(s"ADD COLUMN $name ${tpe.mapToStringName}")).update.run.void
          case ModifyColumn(name, JsonCol) =>
            val tmpCol = "tmp___"
            (fr"ALTER TABLE" ++ dbPath ++
              Fragment.const(s"ADD COLUMN $tmpCol jsonb")).update.run *>
            (fr"UPDATE" ++ dbPath ++
              Fragment.const(s"$tmpCol = ('[' || $name || ']')::jsonb")).update.run *>
            (fr"ALTER TABLE" ++ dbPath ++ fr"DROP COLUMN" ++
              Fragment.const(name)).update.run *>
            (fr"ALTER TABLE" ++ dbPath ++ fr"RENAME COLUMN" ++
              Fragment.const(tmpCol) ++ fr"TO" ++ Fragment.const(name)).update.run.void
          case ModifyColumn(name, tpe) =>
            (fr"ALTER TABLE" ++ dbPath ++
            Fragment.const(s"ALTER COLUMN $name type ${tpe.mapToStringName}")).update.run.void
        }
    }
  }
}
