/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery.syntax._

import monocle.macros.Lenses
import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class NamespaceDecl(prefix: NSPrefix, uri: NSUri) {
  def render: String = s"declare namespace ${prefix.shows} = ${uri.xs.shows}"
}

object NamespaceDecl {
  implicit val order: Order[NamespaceDecl] =
    Order.orderBy(ns => (ns.prefix, ns.uri))

  implicit val show: Show[NamespaceDecl] =
    Show.shows(ns => s"NamespaceDecl(${ns.render})")
}
