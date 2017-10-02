/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms.planner

import doobie.util.fragment.Fragment
import quasar.contrib.pathy.AFile
import quasar.qscript.ShiftedRead

/**
  * Abstracts out SQL expression building.
  */
trait SqlExprBuilder {

  /**
    * IncludeId needs to return a vector of JSONs like: [ (Data.Arr(Data.Int(1), dataRow1), (Data.Arr(Data.Int(2), dataRow12), ... ]
    * IdOnly needs to return a vector of JSONs like: [Data.Int(1), Data.Int(2), ...]
    * ExcludedId needs to return a vector of JSONS like: [ dataRow1, dataRow2, ...]
    */
  def selectAll(semantics: ShiftedRead[AFile]): Fragment
}
