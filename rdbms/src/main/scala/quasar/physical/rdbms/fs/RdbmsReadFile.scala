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

package quasar.physical.rdbms.fs

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.Kvs
import quasar.effect.Kvs._
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.TablePath
import quasar.connector.ManagedReadFile
import quasar.fp.free.lift
import quasar.physical.rdbms.model.DbDataStream
import quasar.fs.impl.{dataStreamClose, dataStreamRead}

import doobie.util.meta.Meta
import doobie.syntax.process._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait RdbmsReadFile
    extends RdbmsDescribeTable
    with RdbmsScanTable
    with ManagedReadFile[DbDataStream] {
  this: Rdbms =>

  import ReadFile._

  implicit def MonadM: Monad[M]
  implicit def dataMeta: Meta[Data]
  def ReadKvsM: Kvs[M, ReadHandle, DbDataStream] =
    Kvs[M, ReadHandle, DbDataStream]

  def ManagedReadFileModule: ManagedReadFileModule = new ManagedReadFileModule {

    override def nextChunk(
        c: DbDataStream): Backend[(DbDataStream, Vector[Data])] = {
      ME.unattempt(
        dataStreamRead(c.stream)
          .map(_.rightMap {
            case (newStream, data) => (c.copy(stream = newStream), data)
          })
          .liftB)
    }

    def getTableCursor(
        dbPath: TablePath,
        cfg: Config,
        offset: Natural,
        limit: Option[Positive]
    ): Backend[DbDataStream] = {
      MRT.ask.map { xa =>
        DbDataStream(
          selectAllQuery(dbPath, offset, limit)
            .query[Data]
            .process
            .chunk(chunkSize)
            .map(_.right[quasar.fs.FileSystemError])
            .transact(xa))
      }.liftB
    }

    override def readCursor(file: AFile,
                            offset: Natural,
                            limit: Option[Positive]): Backend[DbDataStream] = {
      val dbPath = TablePath.create(file)
      for {
        cfg <- MR.ask
        exists <- tableExists(dbPath).liftB
        cursor <- if (exists) getTableCursor(dbPath, cfg, offset, limit)
          else Task.now(DbDataStream.empty).liftB
      } yield cursor
    }

    override def closeCursor(c: DbDataStream): Configured[Unit] = {
      lift(dataStreamClose(c.stream)).into[Eff].liftM[ConfiguredT]
    }
  }
}
