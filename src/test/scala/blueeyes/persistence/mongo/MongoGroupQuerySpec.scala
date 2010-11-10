package blueeyes.persistence.mongo

import org.spex.Specification
import MongoQueryBuilder._
import MongoFilterBuilder._
import MongoImplicits._
import MongoFilterOperators._
import blueeyes.json.JPathImplicits._
import blueeyes.json.JPath
import blueeyes.json.JsonAST.JObject

class MongoGroupQuerySpec extends Specification{
  private val query = group(JObject(Nil), "dummy", "foo", "bar").from("collection")

  "'where' method sets new filter" in {
    import MongoFilterImplicits._
    query.where("name" === "Joe") mustEqual (MongoGroupQuery(MongoSelection(JPath("foo") :: JPath("bar") :: Nil), "collection", JObject(Nil), "dummy", Some(MongoFieldFilter("name", $eq, "Joe"))))
  }
}