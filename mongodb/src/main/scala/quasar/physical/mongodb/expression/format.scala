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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import scalaz._, Scalaz._

sealed abstract class FormatSpecifier(val str: String)
object FormatSpecifier {
  case object Year        extends FormatSpecifier("%Y")
  case object Month       extends FormatSpecifier("%m")
  case object DayOfMonth  extends FormatSpecifier("%d")
  case object Hour        extends FormatSpecifier("%H")
  case object Minute      extends FormatSpecifier("%M")
  case object Second      extends FormatSpecifier("%S")
  case object Millisecond extends FormatSpecifier("%L")
  case object DayOfYear   extends FormatSpecifier("%j")
  case object DayOfWeek   extends FormatSpecifier("%w")
  case object WeekOfYear  extends FormatSpecifier("%U")
}

final case class FormatString(components: List[String \/ FormatSpecifier]) {
  def ::(str: String): FormatString = FormatString(str.left :: components)
  def ::(spec: FormatSpecifier): FormatString = FormatString(spec.right :: components)
}
object FormatString {
  val empty: FormatString = FormatString(Nil)

  implicit val equal: Equal[FormatString] = Equal.equalA
}
