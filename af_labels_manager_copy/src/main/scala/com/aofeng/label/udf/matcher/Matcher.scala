package com.aofeng.label.udf.matcher

import com.aofeng.label.udf.matcher.MatchType.MatchType

/**
 * 每个 Label 的一个 JSON Value 中每个单元对应一个 Matcher
 */
trait Matcher {

  def matches(otherValue: String): Boolean
}

object Matcher {

  def create(matchType: MatchType, value: String): Matcher = {
    matchType match {
      case MatchType.SINGLE => new SingleValueMatcher(value)
      case MatchType.LIST => new ListMatcher(value)
      case MatchType.RANGE => new RangeMatcher(value)
    }
  }
}
