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
import quasar.{Data, DataCodec}
import doobie.util.meta.Meta
import doobie.enum.jdbctype.{JdbcType, VarChar}
import doobie.util.composite.Composite
import doobie.util.kernel.Kernel
import java.sql.{PreparedStatement, ResultSet}

import doobie.enum.jdbctype
import org.postgresql.util.PGobject

import scalaz.syntax.show._
import scalaz.syntax.equal._
import scalaz.std.string._

package object mapping {

  implicit val codec = DataCodec.Precise

  implicit val JsonDataMeta: Meta[Data] =
    Meta.other[PGobject]("json").xmap[Data](
      pGobject =>
        DataCodec.parse(pGobject.getValue).valueOr(err => scala.sys.error(err.shows)), // failure raises an exception
      data => {
        val o = new PGobject
        o.setType("json")
        o.setValue(DataCodec.render(data).getOrElse("{}"))
        o
      }
    )

  /**
    * Use to deserialize rows where multiple columns are expected.
    */
  object Composite {
    implicit val dataComposite: Composite[Data] = new Composite[Data] {
      val kernel = new Kernel[Data] {
        type I = Data
        val ia = (i: I) => i
        val ai = (a: I) => a
        val get = (rs: ResultSet, _: Int) => {
          val rsMeta = rs.getMetaData
          val cc = rsMeta.getColumnCount

          val cols = (1 to cc).map { index =>
              val colType = JdbcType.fromInt(rsMeta.getColumnType(index))
              if (Option(rs.getString(index)).isEmpty)
                Data.Null
              else colType match {
                case VarChar => Data.Str(rs.getString(index))
                case jdbctype.Integer => Data.Int(rs.getInt(index))
                case _ if rsMeta.getColumnName(index) === "data" =>
                  JsonDataMeta.unsafeGetNonNullable(rs, index)
                case _ =>
                  val colName = rsMeta.getColumnName(index)
                  scala.sys.error(s"Unsupported column $index ($colName) in ResultSet")
              }
          }
          cols.toList match {
            case Nil =>
              Data.Null
            case single :: Nil =>
              single
            case columnList =>
              Data.Arr(columnList)
          }
        }
        val set = (_: PreparedStatement, _: Int, _: I) => ()
        val setNull = (_: PreparedStatement, _: Int) => ()
        val update = (_: ResultSet, _: Int, _: I) => ()
        val width = 0
      }
      val meta = Nil
      val toList = (d: Data) => List(d)
    }
  }
}
