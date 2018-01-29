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

package quasar.main.api

import slamdata.Predef._
import quasar.{Data, QuasarError}
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.stream._
import quasar.db.DbConnectionConfig
import quasar.fs.FileSystemError.pathErr
import quasar.fs.PathError.pathNotFound
import quasar.fs.mount._
import quasar.fp.ski._
import quasar.main._
import quasar.metastore.MetaStore.{ShouldCopy, ShouldInitialize}
import quasar.metastore.MetaStoreFixture
import quasar.sql._

import scala.StringContext
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class ApiSpec extends quasar.Qspec {

  def newQuasar(metaConfig: Option[DbConnectionConfig]): Task[Quasar] =
    metaConfig.fold(MetaStoreFixture.createNewTestMetaStoreConfig)(Task.now(_)).flatMap { config =>
      Quasar.initWithDbConfig(
        BackendConfig.Empty,
        config,
        _ => ().point[MainTask]).leftMap(e => new scala.Exception(e.shows)).run.unattempt
    }

  def testProgram[A](metaConfig: Option[DbConnectionConfig])(test: Free[CoreEff, A]): Task[A] =
    for {
      quasarFS <- newQuasar(metaConfig)
      q        <- quasarFS.taskInter
      result   <- test.foldMap(q).onFinish(κ(quasarFS.shutdown))
    } yield result

  def testProgramErrors[A](metaConfig: Option[DbConnectionConfig])(test: Free[CoreEff, A]): Task[QuasarError \/ A] =
    for {
      quasarFS <- newQuasar(metaConfig)
      q        <- quasarFS.interpWithErrs
      result   <- test.foldMap(q).run.onFinish(κ(quasarFS.shutdown))
    } yield result

  "Api" >> {
    "change metastore at runtime" >> {
      val sampleMountPath = rootDir </> file("foo")
      val sampleMount = MountConfig.viewConfig0(sqlB"select * from zips")
      val mount = Mounting.Ops[CoreEff]
      (for {
        firstMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
        otherMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig

        result <- testProgram(firstMetaConf.some) {
          for {
            _             <- mount.mountOrReplace(sampleMountPath, sampleMount, false)
            mountIsThere  <- mount.lookupConfig(sampleMountPath).run.run.map(_.isDefined)
            _             <- MetaStoreLocation.Ops[CoreEff].set(otherMetaConf, initialize = ShouldInitialize(true), copy = ShouldCopy(false))
            noLongerThere <- mount.lookupConfig(sampleMountPath).run.run.map(_.empty)
            _             <- MetaStoreLocation.Ops[CoreEff].set(firstMetaConf, initialize = ShouldInitialize(true), copy = ShouldCopy(false))
            thereAgain    <- mount.lookupConfig(sampleMountPath).run.run.map(_.isDefined)
          } yield {
            mountIsThere must_=== true
            noLongerThere must_=== true
            thereAgain must_=== true
          }
        }
      } yield result).unsafePerformSync
    }

    "attempting to change the metastore to the same one succeeds without doing anything" >> {
      (for {
        metaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
        result <- testProgram(metaConf.some) {
          MetaStoreLocation.Ops[CoreEff].set(metaConf, initialize = ShouldInitialize(true), copy = ShouldCopy(false))
        }
      } yield result must_= ().right).unsafePerformSync
    }
    "can create a view even if it references a non-existent file and get an error when" >> {
      "querying" >> {
        testProgram(None)(for {
          _      <- QuasarAPI.createView(rootDir </> file("badView"), sqlB"select * from noThing")
          result <- QuasarAPI.query(rootDir, sqlB"select * from badView")
        } yield result must_=== pathErr(pathNotFound(rootDir </> file("noThing"))).left.right).unsafePerformSync
      }
      "or reading the view" >> {
        testProgramErrors(None)(for {
          _      <- QuasarAPI.createView(rootDir </> file("badView"), sqlB"select * from noThing")
          result <- QuasarAPI.readFile(rootDir </> file("badView")).runLogCatch
        } yield result).unsafePerformSync must_===
          pathErr(pathNotFound(rootDir </> file("noThing"))).left
      }
    }
    "create a view that selects a literal and read it later" >> {
      testProgram(None)(for {
        _      <- QuasarAPI.createView(rootDir </> file("constantView"), sqlB"""select "Hello World!"""")
        result <- QuasarAPI.readFile(rootDir </> file("constantView")).runLogCatch
      } yield result must_=== Vector(Data.Str("Hello World!")).right).unsafePerformSync
    }
    "create a view that selects a literal and query it later" >> {
      testProgram(None)(for {
        _      <- QuasarAPI.createView(rootDir </> file("constantView"), sqlB"""select "Hello World!"""")
        result <- QuasarAPI.queryVec(rootDir, sqlB"select * from constantView")
      } yield result must_=== Vector(Data.Str("Hello World!")).right.right).unsafePerformSync
    }
  }
}
