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

package quasar.physical.rdbms

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo

import pathy.Path
import pathy.Path.DirName
import scalaz._
import Scalaz._

object common {
  final case class Config(connInfo: JdbcConnectionInfo)

  final case class Schema(name: String) {

    def isRoot: Boolean = name.isEmpty

    private def lastDirNameStr: String = {
      val lastSeparatorIndex = name.lastIndexOf(TablePath.Separator)
      val startIndex =
        if (lastSeparatorIndex > 0)
          lastSeparatorIndex + TablePath.Separator.length
        else 0
      name.substring(startIndex)
    }

    def lastDirName: DirName = {
      DirName(TablePath.unescapeSchemaName(lastDirNameStr))
    }

    def isDirectChildOf(supposedParent: Schema): Boolean = {
      supposedParent.name match {
        case "" =>
          !this.name.contains(TablePath.Separator)
        case parentName =>
          this.name === parentName + TablePath.Separator + lastDirNameStr
      }
    }

    def parents: List[Schema] = {
      name
        .split(TablePath.SeparatorRegex)
        .toList
        .dropRight(1)
        .scanLeft(List.empty[String])(_ :+ _)
        .drop(1)
        .map(dirs => dirs.mkString(TablePath.Separator))
        .map(Schema.apply)
    }
  }

  implicit val showSchema: Show[Schema] = Show.shows(_.name.toLowerCase)

  final case class TableName(name: String) extends AnyVal
  final case class TablePath(schema: Schema, table: TableName)

  object TablePath {

    val Separator = "__c_"
    val SeparatorRegex = "__c_"

    def escapeSchemaName(dirStr: String): String = {
      dirStr.replace(".", "$d$")
    }

    def unescapeSchemaName(dirStr: String): String = {
      dirStr.replace("$d$", ".")
    }

    def dirToSchema(dir: ADir): Schema = {
      Schema(
        Path
          .flatten(None, None, None, Some(_), Some(_), dir)
          .toIList
          .unite
          .map(escapeSchemaName)
          .intercalate(TablePath.Separator))
    }

    def create(file: AFile): TablePath = {
      val filename = Path.fileName(file).value
      val schema =
        Path.parentDir(file).map(dirToSchema).getOrElse(Schema(""))
      new TablePath(schema, TableName(filename))
    }

    implicit val showTableName: Show[TableName] =
      Show.shows(_.name.toLowerCase)

    implicit val showPath: Show[TablePath] = Show.shows { tp =>
      s"${tp.schema.shows}.${tp.table.shows}"
    }
  }
}
