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

package quasar.api

import quasar.Predef._
import quasar.contrib.pathy._

import org.http4s.dsl.{Path => HPath}
import org.specs2.specification.core._
import pathy.scalacheck.PathyArbitrary._

/** This is largely copied from `pathy`. It would be nice to expose from a pathy
  * a function to validate a `PathCodec`. */
class UriPathCodecSpec extends quasar.Qspec {
  val codec = UriPathCodec

  override def map(fs: => Fragments) = encodingFragments append super.map(fs)

  private def encodings = List(
    "%"   -> "%25",
    "%2"  -> "%252",
    "%25" -> "%2525",
    "%a"  -> "%25a",
    "%aa" -> "%25aa",
    "%ag" -> "%25ag",
    "%AA" -> "%25AA",
    "%AG" -> "%25AG",
    "."   -> "%2E",
    ".."  -> "%2E%2E",
    "..." -> "...",
    "/"   -> "%2F",
    " "   -> "%20",
    "+ "  -> "%2B%20"
  )

  private def encodingFragments = Fragments(
       fragmentFactory.section("encode special characters properly")
    +: fragmentFactory.break
    +: (encodings map checkEncoding)
    :+ fragmentFactory.break
    : _*
  )

  private def checkEncoding(pair: (String, String)): Fragment = {
    val (from, to) = pair
    val result = (
         ((codec escape from) must_=== to)
      && ((codec unescape to) must_=== from)
    )
    fragmentFactory.example(f"$from%-8s -> $to%-8s -> $from%-8s\n", result)
  }

  "UriPathCodec" should {
    "print and parse again should produce same Path" >> {
      "absolute file" >> prop { path: AFile =>
        codec.parseAbsFile(codec.printPath(path)) must_== Some(path)
      }
      "relative file" >> prop { path: RFile =>
        codec.parseRelFile(codec.printPath(path)) must_== Some(path)
      }
      "absolute dir" >> prop { path: ADir =>
        codec.parseAbsDir(codec.printPath(path)) must_== Some(path)
      }
      "relative dir" >> prop { path: RDir =>
        codec.parseRelDir(codec.printPath(path)) must_== Some(path)
      }
    }

    "print and parse through http4s should produce same Path" >> {
      "absolute file with plus" >> {
        import pathy.Path._
        val path = rootDir </> file("a+b/c")
        val hpath = HPath(codec.printPath(path))
        AsFilePath.unapply(hpath) must_== Some(path)
      }

      "absolute dir with plus" >> {
        import pathy.Path._
        val path = rootDir </> dir("a+b/c")
        val hpath = HPath(codec.printPath(path))
        AsDirPath.unapply(hpath) must_== Some(path)
      }

      "absolute file" >> prop { path: AFile =>
        val hpath = HPath(codec.printPath(path))
        AsFilePath.unapply(hpath) must_== Some(path)
      }
      "absolute dir" >> prop { path: ADir =>
        val hpath = HPath(codec.printPath(path))
        AsDirPath.unapply(hpath) must_== Some(path)
      }
    }
  }
}
