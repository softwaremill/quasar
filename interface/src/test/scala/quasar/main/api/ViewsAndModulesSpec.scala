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
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.fp.free.foldMapNT
import quasar.fs.{FileSystemErrT, ReadFile}
import quasar.fs.mount.{MountConfig, MountingsConfig}
import quasar.main.CoreEff
import quasar.main.Fixture._
import quasar.sql._

import pathy.Path._
import scalaz._, Scalaz._

class ViewsAndModulesSpec extends quasar.Qspec {

  "Views and modules" >> {
    "A view with a relative import should resolve relative to the directory in which it resides" >> {
      val view =  MountConfig.viewConfig0(sqlB"IMPORT `./foo/`; bar(true)")
      val viewFile: AFile = rootDir </> dir("a") </> file("view")
      val module = MountConfig.moduleConfig(sqlM"CREATE FUNCTION BAR(:h) BEGIN 1 END")
      val moduleDir: ADir = rootDir </> dir("a") </> dir("foo")
      val mounts = Map[APath, MountConfig](viewFile -> view, moduleDir -> module)
      val interp = foldMapNT(inMemFSEvalSimple(mounts = MountingsConfig(mounts)).unsafePerformSync)
      val program = ReadFile.Ops[CoreEff].scanAll(viewFile).translate(Hoist[FileSystemErrT].hoist(interp))
      program.runLog.run.unsafePerformSync must_= Vector(Data.Int(1)).right
    }
  }

}
