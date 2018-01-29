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

import slamdata.Predef.String
import quasar.qscript._
import quasar.contrib.scalaz.MonadError_
import quasar.contrib.pathy.{ADir, AFile}

import scalaz.{Const, NonEmptyList, MonadError}

package object marklogic {
  type ErrorMessages = NonEmptyList[String]

  type MonadErrMsgs[F[_]]  = MonadError[F, ErrorMessages]

  object MonadErrMsgs {
    def apply[F[_]](implicit F: MonadErrMsgs[F]): MonadErrMsgs[F] = F
  }

  type MonadErrMsgs_[F[_]] = MonadError_[F, ErrorMessages]

  object MonadErrMsgs_ {
    def apply[F[_]](implicit F: MonadErrMsgs_[F]): MonadErrMsgs_[F] = F
  }

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[fs.MLQScriptCP[T]#M, QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[Read[AFile], ?]]))
  
  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable.Aux[QScriptCore[T, ?], fs.MLQScriptCP[T]#M] =
    Injectable.inject[QScriptCore[T, ?], fs.MLQScriptCP[T]#M]

  implicit def thetaJoinToQScript[T[_[_]]]: Injectable.Aux[ThetaJoin[T, ?], fs.MLQScriptCP[T]#M] =
    Injectable.inject[ThetaJoin[T, ?], fs.MLQScriptCP[T]#M]

  implicit def readToQScript[T[_[_]]]: Injectable.Aux[Const[Read[AFile], ?], fs.MLQScriptCP[T]#M] =
    Injectable.inject[Const[Read[AFile], ?], fs.MLQScriptCP[T]#M]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable.Aux[Const[ShiftedRead[ADir], ?], fs.MLQScriptCP[T]#M] =
    Injectable.inject[Const[ShiftedRead[ADir], ?], fs.MLQScriptCP[T]#M]


}
