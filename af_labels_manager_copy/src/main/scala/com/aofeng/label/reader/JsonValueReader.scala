package com.aofeng.label.reader

class JsonValueReader extends Reader[String, Map[String, MatchRule]] {

  override def read(in: String): Map[String, MatchRule] = {
    import org.json4s._
    import org.json4s.jackson.Serialization

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    Serialization.read[Map[String, MatchRule]](in)
  }

}

case class MatchRule(value: String, `type`: String)
