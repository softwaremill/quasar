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
import quasar.contrib.pathy.APath
import quasar.effect.{KeyValueStore, Read, Write}
import quasar.fp._
import quasar.fp.free._
import quasar.fs.InMemory
import quasar.fs.InMemory.InMemState
import quasar.fs.mount._
import quasar.fs.mount.cache.VCache
import quasar.metastore.{MetaStore, MetaStoreFixture}

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object Fixture {
  def mountingInter(mounts: Map[APath, MountConfig]): Task[Mounting ~> Task] =
    mountingInterInspect(mounts).map{ case (inter, ref) => inter }

  def mountingInterInspect(mounts: Map[APath, MountConfig]): Task[(Mounting ~> Task, Task[Map[APath, MountConfig]])] = {
    type MEff[A] = Coproduct[Task, MountConfigs, A]
    TaskRef(mounts).map { configsRef =>

      val mounter: Mounting ~> Free[MEff, ?] = Mounter.trivial[MEff]

      val meff: MEff ~> Task =
        reflNT[Task] :+: KeyValueStore.impl.fromTaskRef(configsRef)

      (foldMapNT(meff) compose mounter, configsRef.read)
    }
  }

  def inMemFS(
    state: InMemState = InMemState.empty,
    mounts: MountingsConfig = MountingsConfig.empty
  ): Task[FS] =
    inMemFSInspect(state, mounts).map { case (inter, ref) => inter }

  def inMemFSInspect(
    state: InMemState = InMemState.empty,
    mounts: MountingsConfig = MountingsConfig.empty
  ): Task[(FS, Task[(InMemState, Map[APath, MountConfig])])] = {
    val noShutdown: Task[Unit] = Task.now(())
    (InMemory.runBackendInspect(state) |@| mountingInterInspect(mounts.toMap))((fsAndRef, mountAndRef) =>
      (FS(
        fsAndRef._1 andThen injectFT[Task, QErrs_Task],
        mountAndRef._1 andThen injectFT[Task, QErrs_Task],
        noShutdown), (fsAndRef._2 |@| mountAndRef._2).tupled))
  }

  def inMemFSEvalInspect(
    state: InMemState = InMemState.empty,
    mounts: MountingsConfig = MountingsConfig.empty,
    metaRefT: Task[TaskRef[MetaStore]] = MetaStoreFixture.createNewTestMetastore().flatMap(TaskRef(_)),
    persist: quasar.db.DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]
  ): Task[(CoreEff ~> QErrs_TaskM, Task[(InMemState, Map[APath, MountConfig])])] =
    for {
      r         <- TaskRef(Tags.Min(Option.empty[VCache.Expiration]))
      metaRef   <- metaRefT
      result    <- inMemFSInspect(state, mounts)
      (fs, ref) = result
      eval      <- CoreEff.defaultImpl(fs, metaRef, persist)
    } yield
      (eval andThen
        foldMapNT(
          (Read.fromTaskRef(r) andThen injectFT[Task, QErrs_Task])  :+:
            (Write.fromTaskRef(r) andThen injectFT[Task, QErrs_Task]) :+:
            injectFT[Task, QErrs_Task]                                :+:
            injectFT[QErrs, QErrs_Task]), ref)

  def inMemFSEval(
    state: InMemState = InMemState.empty,
    mounts: MountingsConfig = MountingsConfig.empty,
    metaRefT: Task[TaskRef[MetaStore]] = MetaStoreFixture.createNewTestMetastore().flatMap(TaskRef(_)),
    persist: quasar.db.DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]
  ): Task[CoreEff ~> QErrs_TaskM] =
    inMemFSEvalInspect(state, mounts, metaRefT, persist).map{ case (inter, ref) => inter }

  def inMemFSEvalSimple(
    state: InMemState = InMemState.empty,
    mounts: MountingsConfig = MountingsConfig.empty,
    metaRefT: Task[TaskRef[MetaStore]] = MetaStoreFixture.createNewTestMetastore().flatMap(TaskRef(_)),
    persist: quasar.db.DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]
  ): Task[CoreEff ~> Task] =
    inMemFSEval(state, mounts, metaRefT, persist)
      .map(_ andThen foldMapNT(reflNT[Task] :+: QErrs.toCatchable[Task]))
}
