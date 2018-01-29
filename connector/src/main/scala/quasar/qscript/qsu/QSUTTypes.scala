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

package quasar.qscript.qsu

import quasar.ejson.EJson
import quasar.qscript.TTypes

trait QSUTTypes[T[_[_]]] extends TTypes[T] {
  type QAuth = quasar.qscript.qsu.QAuth[T]
  type QDims = quasar.qscript.qsu.QDims[T]
  type QIdAccess = quasar.qscript.qsu.QIdAccess[T]
  type QAccess[A] = quasar.qscript.qsu.QAccess[T, A]
  type FreeAccess[A] = quasar.qscript.qsu.FreeAccess[T, A]
  type QSUGraph = quasar.qscript.qsu.QSUGraph[T]
  type RevIdx = quasar.qscript.qsu.QSUGraph.RevIdx[T]
  type RevIdxM[F[_]] = quasar.qscript.qsu.RevIdxM[T, F]
  type References = quasar.qscript.qsu.References[T, T[EJson]]
  type QScriptUniform[A] = quasar.qscript.qsu.QScriptUniform[T, A]
  type QScriptEducated[A] = quasar.qscript.QScriptEducated[T, A]
}
