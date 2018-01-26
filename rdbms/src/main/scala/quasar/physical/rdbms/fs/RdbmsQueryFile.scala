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

package quasar.physical.rdbms.fs

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile, PathSegment}
import quasar.{Data, RenderTreeT}
import quasar.effect.Kvs
import quasar.fp.free.lift
import quasar.fs.impl.{dataStreamClose, dataStreamRead}
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.{FileSystemErrT, QueryFile}
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common._
import quasar.physical.rdbms.common.TablePath.showTableName
import quasar.connector.ManagedQueryFile
import quasar.physical.rdbms.model.DbDataStream
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql.SqlExpr

import doobie.syntax.process._
import doobie.util.fragment.Fragment
import matryoshka.data.Fix
import pathy.Path
import scalaz._
import Scalaz._
import scalaz.stream.Process._

trait RdbmsQueryFile extends ManagedQueryFile[DbDataStream] with RdbmsMove {
  self: Rdbms =>

  import QueryFile._
  implicit def MonadM: Monad[M]
  override def ResultKvsM: Kvs[M, ResultHandle, DbDataStream] = Kvs[M, ResultHandle, DbDataStream]
  def renderQuery: RenderQuery // TODO this should be chosen based on schema

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  override def ManagedQueryFileModule: ManagedQueryFileModule = new ManagedQueryFileModule {

    override def explain(repr: Fix[SqlExpr]): Backend[String] = {
      val sqlExprTreeString = RenderTreeT[Fix].render(repr).shows
      ME.unattempt(renderQuery.asString(repr)
        .leftMap(QScriptPlanningFailed.apply)
        .traverse { q =>
          val treeAndQueryStr = s"SqlExprTree:\n$sqlExprTreeString\n\nRaw query:\n$q"
          (Fragment.const("EXPLAIN") ++ Fragment.const(q))
            .query[String].list.map(~_.headOption).liftB
            .valueOr(err => s"unavailable (${err.shows})").liftM[FileSystemErrT].map { dbOutput =>
            s"$treeAndQueryStr\n\nDB EXPLAIN output:\n$dbOutput"
          }
        })
    }

    override def executePlan(repr: Fix[SqlExpr], out: AFile): Backend[Unit] = {
      val tablePath = TablePath.create(out)
      ME.unattempt(renderQuery.asString(repr)
        .leftMap(QScriptPlanningFailed.apply)
        .traverse { q =>
          val cmd = Fragment.const("CREATE TABLE") ++
          Fragment.const(tablePath.shows) ++
          Fragment.const("AS") ++
          Fragment.const(q)
          (dropTableIfExists(tablePath) *> cmd.update.run).liftB
      }).void
    }

    override def resultsCursor(repr: Fix[SqlExpr]): Backend[DbDataStream] = {
      ME.unattempt(renderQuery.asString(repr)
        .leftMap(QScriptPlanningFailed.apply)
        .traverse { qStr =>
        MRT.ask.map { xa =>
            DbDataStream(Fragment.const(qStr)
              .query[Data]
              .process
              .chunk(chunkSize)
              .attempt(ex =>
                emit(readFailed(qStr, ex.getLocalizedMessage)))
              .transact(xa))
          }.liftB
     })
    }

    override def nextChunk(c: DbDataStream): Backend[(DbDataStream, Vector[Data])] =
      ME.unattempt(
        dataStreamRead(c.stream)
          .map(_.rightMap {
            case (newStream, data) => (c.copy(stream = newStream), data)
          })
          .liftB)

    override def fileExists(file: AFile): Configured[Boolean] =
      lift(tableExists(TablePath.create(file))).into[Eff].liftM[ConfiguredT]

    override def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      val schema = TablePath.dirToSchema(dir)
      schemaExists(schema).liftB.flatMap(_.unlessM(ME.raiseError(pathErr(pathNotFound(dir))))) *>
        (for {
        childSchemas <- findChildSchemas(schema)
        childTables <- findChildTables(schema)
        childDirs = childSchemas.filter(_.isDirectChildOf(schema)).map(d => -\/(d.lastDirName)).toSet
        childFiles = childTables.map(t => \/-(Path.FileName(t.shows))).toSet
      }
        yield childDirs ++ childFiles).liftB
    }

    override def closeCursor(c: DbDataStream): Configured[Unit] =
      lift(dataStreamClose(c.stream)).into[Eff].liftM[ConfiguredT]
  }
}
