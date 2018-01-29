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

package quasar.fs

import slamdata.Predef.Vector
import quasar.contrib.pathy._
import quasar.Data
import quasar.effect._
import quasar.fp._, free._
import quasar.fs.mount.cache.VCache.VCacheKVS

import scalaz.{Failure => _, _}
import scalaz.concurrent.Task

package object mount {
  type MntErrT[F[_], A] = EitherT[F, MountingError, A]

  type MountingFailure[A] = Failure[MountingError, A]

  object MountingFailure {
    def Ops[S[_]](implicit S: MountingFailure :<: S) =
      Failure.Ops[MountingError, S]
  }

  type PathMismatchFailure[A] = Failure[Mounting.PathTypeMismatch, A]

  object PathMismatchFailure {
    def Ops[S[_]](implicit S: PathMismatchFailure :<: S) =
      Failure.Ops[Mounting.PathTypeMismatch, S]
  }

  type MountConfigs[A] = KeyValueStore[APath, MountConfig, A]

  object MountConfigs {
    def Ops[S[_]](implicit S: MountConfigs :<: S) =
      KeyValueStore.Ops[APath, MountConfig, S]
  }


  //-- Views --

  type ViewFileSystem[A] = (
        Mounting
    :\: PathMismatchFailure
    :\: MountingFailure
    :\: view.State
    :\: VCacheKVS
    :\: MonotonicSeq
    :/: FileSystem
  )#M[A]

  object ViewFileSystem {
    def interpret[F[_]](
      mounting: Mounting ~> F,
      mismatchFailure: PathMismatchFailure ~> F,
      mountingFailure: MountingFailure ~> F,
      viewState: view.State ~> F,
      vcache: VCacheKVS ~> F,
      monotonicSeq: MonotonicSeq ~> F,
      fileSystem: FileSystem ~> F
    ): ViewFileSystem ~> F =
      mounting :+: mismatchFailure :+: mountingFailure :+: viewState :+: vcache :+: monotonicSeq :+: fileSystem
  }

  def overlayModulesViews[S[_], T[_]](
    f: BackendEffect ~> Free[T, ?]
  )(implicit
    S0: T :<: S,
    S1: Task :<: S,
    S2: VCacheKVS :<: S,
    S3: Mounting :<: S,
    S4: MountingFailure :<: S,
    S5: PathMismatchFailure :<: S
  ): Task[BackendEffect ~> Free[S, ?]] = {
    type V[A] = (
      constantPlans.State :\:
      VCacheKVS           :\:
      view.State          :\:
      MonotonicSeq        :\:
      Mounting            :\:
      MountingFailure     :\:
      PathMismatchFailure :/:
      BackendEffect)#M[A]

    for {
      startSeq <- Task.delay(scala.util.Random.nextInt.toLong)
      seq      <- MonotonicSeq.from(startSeq)
      viewKvs  <- KeyValueStore.impl.default[ReadFile.ReadHandle, view.ResultHandle]
      constKvs <- KeyValueStore.impl.default[QueryFile.ResultHandle, Vector[Data]]
    } yield {
      val compFs: V ~> Free[S, ?] =
        injectFT[Task, S].compose(constKvs)                                 :+:
        injectFT[VCacheKVS, S]                                              :+:
        injectFT[Task, S].compose(viewKvs)                                  :+:
        injectFT[Task, S].compose(seq)                                      :+:
        injectFT[Mounting, S]                                               :+:
        injectFT[MountingFailure, S]                                        :+:
        injectFT[PathMismatchFailure, S]                                    :+:
        (foldMapNT(injectFT[T, S]) compose f)

      flatMapSNT(compFs) compose
        // The constant plan interpreter should appear last before the actual filesystem
        // so that it can catch any plans that become constant after view and modules are
        // involved
        flatMapSNT(transformIn[QueryFile, V, Free[V, ?]](constantPlans.queryFile[V], liftFT)) compose
        flatMapSNT(transformIn[BackendEffect, V, Free[V, ?]](module.backendEffect[V], liftFT)) compose
        view.backendEffect[V]
    }
  }
}
