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

package quasar.fs.mount

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.fs._, FileSystemError._, PathError._, MountType._
import quasar.fs.mount.cache.VCache.VCacheKVS

import pathy.Path._
import scalaz.{Failure => _, Node => _, _}, Scalaz._

package object module {

  /** Intercept and fail any read to a module path; all others are passed untouched. */
  def readFile[S[_]](
    implicit
    S0: ReadFile :<: S,
    S4: Mounting :<: S
  ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    λ[ReadFile ~> Free[S, ?]] {
      case Open(file, off, lim) =>
        mount.havingPrefix(rootDir).flatMap { map =>
          val modules = map.filter { case (k, v) => v ≟ ModuleMount.right }
          if(modules.keys.exists { path => refineType(path).fold(dir => file.relativeTo(dir).isDefined, _ => false) })
            pathErr(invalidPath(file, "Cannot read file in a module.")).left.point[Free[S, ?]]
          else readUnsafe.open(file, off, lim).run
        }
      case Read(handle)  => readUnsafe.read(handle).run
      case Close(handle) => readUnsafe.close(handle)
    }
  }

  /** Intercept and fail any write to a module path; all others are passed untouched. */
  def writeFile[S[_]](
    implicit
    S0: WriteFile :<: S,
    S1: Mounting :<: S
  ): WriteFile ~> Free[S, ?] = {
    val mount = Mounting.Ops[S]
    nonFsMounts.failSomeWrites(
      on = file => mount.lookupType(file).run.run.map(_.filter(_ ≟ ModuleMount.right).isDefined),
      message = "Cannot write to a module.")
  }

  /** Overlay modules when enumerating files and directories. */
  def queryFile[S[_]](
    implicit
    S0: QueryFile :<: S,
    S1: Mounting :<: S
  ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    /* Returns any module contained within this directory as a `Node.Module`
     * as well as a `Node.ImplicitDir` for any directory that contains a
     * module somewhere within it
     */
    def listModules(dir: ADir): Free[S, Set[Node]] =
      mount.modulesHavingPrefix_(dir).map(_.foldMap(d =>
        if (depth(d) ≟ 1) dirName(d).map[Node](Node.Module(_)).toSet
        else firstSegmentName(d).flatMap(_.swap.toOption).map[Node](Node.ImplicitDir(_)).toSet))

    def moduleFiles(dir: ADir): OptionT[Free[S, ?], Set[Node]] =
      mount.lookupModuleConfigIgnoreError(dir).map(moduleConfig =>
          moduleConfig.declarations.map(funcDef =>
            Node.Function(FileName(funcDef.name.value))).toSet)

    λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        query.execute(lp, out).run.run

      case EvaluatePlan(lp) =>
        queryUnsafe.eval(lp).run.run

      case More(handle) =>
        queryUnsafe.more(handle).run

      case Close(handle) =>
        queryUnsafe.close(handle)

      case Explain(lp) =>
        query.explain(lp).run.run

      case ListContents(dir) =>
        // If this directory is a module, return the "files" (functions) within it
        // Otherwise add modules and directories containing modules to the underlying call to ls
        moduleFiles(dir).map(_.right[FileSystemError]).getOrElseF {
          (listModules(dir) |@| query.ls(dir).run) ((mls, qls) => qls match {
            case \/-(ps) =>
              (ps ++ mls).right
            case -\/(err@PathErr(PathNotFound(_))) =>
              if (mls.nonEmpty) mls.right else err.left
            case -\/(v) =>
              v.left
          })
        }

      case FileExists(file) =>
          query.fileExists(file)
    }
  }

  def fileSystem[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S6: VCacheKVS :<: S,
    S7: Mounting :<: S,
    S8: MountingFailure :<: S,
    S9: PathMismatchFailure :<: S
  ): FileSystem ~> Free[S, ?] = {
    val mount = Mounting.Ops[S]
    // Module is a directory so we want to add "ourselves" to the result of `modulesHavingPrefix`
    val manageFile = nonFsMounts.manageFile { dir =>
      (mount.modulesHavingPrefix_(dir).map(paths => paths.map(p => (p: RPath))) |@| mount.lookupConfig(dir).toOption.run.run)((children, self) =>
        children ++ self.join.as(currentDir).toSet)
    }
    interpretFileSystem[Free[S, ?]](queryFile, readFile, writeFile, manageFile)
  }
  // FIX-ME
  def backendEffect[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S6: VCacheKVS :<: S,
    S7: Mounting :<: S,
    S8: MountingFailure :<: S,
    S9: PathMismatchFailure :<: S,
    S10: Analyze :<: S
  ): BackendEffect ~> Free[S, ?] = {
    (injectFT[Analyze, S]) :+: fileSystem[S]
  }
}
