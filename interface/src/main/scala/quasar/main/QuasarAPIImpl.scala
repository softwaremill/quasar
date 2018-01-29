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

package quasar.main

import slamdata.Predef._
import quasar.{Data, resolveImports, Variables, SemanticError}
import quasar.contrib.pathy._
import quasar.sql._
import quasar.fp.free.foldMapNT
import quasar.fs.{FileSystemError, QueryFile, ReadFile, Node}
import quasar.fs.mount.{Mounting, MountConfig, MountingError}
import quasar.fs.mount.module.Module

import matryoshka.data.Fix
import eu.timepit.refined.auto._
import scalaz._, Scalaz._
import scalaz.stream.{Process, Process0}

/**
  * The top level Quasar programmatic API. Not yet complete, but will be made
  * more exhaustive over time.
  */
final case class QuasarAPIImpl[F[_]: Monad](inter: CoreEff ~> F) {

  private val mount = Mounting.Ops[CoreEff]
  private val fsQ = new FilesystemQueries[CoreEff]

  def createView(path: AFile, sql: ScopedExpr[Fix[Sql]]): F[Unit] =
    mount.mount(path, MountConfig.viewConfig(sql, Variables.empty)) foldMap inter

  def createModule(path: ADir, statements: List[Statement[Fix[Sql]]]) =
    mount.mount(path, MountConfig.moduleConfig(statements)) foldMap inter

  def getMount(path: APath): F[Option[MountingError \/ MountConfig]] =
    mount.lookupConfig(path).run.run.foldMap(inter)

  def ls(path: ADir): F[FileSystemError \/ Set[Node]] =
    QueryFile.Ops[CoreEff].ls(path).run.foldMap(inter)

  def invoke(function: AFile, args: Map[String, Fix[Sql]]) =
    Module.Ops[CoreEff].invokeFunction_(function, args, offset = 0L, limit = None).translate(foldMapNT(inter))

  def readFile(path: AFile): Process[F, Data] =
    ReadFile.Ops[CoreEff].scanAll_(path).translate(foldMapNT(inter))

  def query(workingDir: ADir, sql: ScopedExpr[Fix[Sql]]): F[NonEmptyList[SemanticError] \/ (FileSystemError \/ Process0[Data])] =
    (for {
      expr   <- resolveImports[CoreEff](sql, workingDir).leftMap(_.wrapNel)
      stream <- EitherT(fsQ.queryResults(expr, Variables.empty, workingDir, off = 0L, lim = None).run.run.value)
    } yield stream).run.foldMap(inter)

  def queryVec(workingDir: ADir, sql: ScopedExpr[Fix[Sql]]): F[NonEmptyList[SemanticError] \/ (FileSystemError \/ Vector[Data])] =
    query(workingDir, sql).map(x => x.map(y => y.map(_.toVector)))
}
