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

package quasar.physical

import quasar.fp._
import quasar.qscript._
import scalaz.Const
import quasar.contrib.pathy.AFile

package object rdbms {
  type PostgresQScriptCP[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[
  PostgresQScriptCP[T]#M,
  QScriptTotal[T, ?]] =
  ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable.Aux[QScriptCore[T, ?], PostgresQScriptCP[T]#M] =
  Injectable.inject[QScriptCore[T, ?], PostgresQScriptCP[T]#M]

  implicit def equiJoinToQScript[T[_[_]]]: Injectable.Aux[EquiJoin[T, ?], PostgresQScriptCP[T]#M] =
  Injectable.inject[EquiJoin[T, ?], PostgresQScriptCP[T]#M]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable.Aux[Const[ShiftedRead[AFile], ?], PostgresQScriptCP[T]#M] =
  Injectable.inject[Const[ShiftedRead[AFile], ?], PostgresQScriptCP[T]#M]
}