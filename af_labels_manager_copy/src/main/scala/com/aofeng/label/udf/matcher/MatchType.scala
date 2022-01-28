package com.aofeng.label.udf.matcher

object MatchType extends Enumeration {

  type MatchType = Value

  val SINGLE: MatchType = Value("1")
  val LIST: MatchType = Value("3")
  val RANGE: MatchType = Value("2")

  def fromName(name: String): MatchType = {
    name.toUpperCase() match {
      case "SINGLE" => SINGLE
      case "LIST" => LIST
      case "RANGE" => RANGE
      case _ => null
    }
  }
}
