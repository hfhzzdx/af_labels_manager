package com.aofeng.label.udf.matcher

class SingleValueMatcher(value: String) extends Matcher {

  override def matches(otherValue: String): Boolean = {
    otherValue equals value
  }
}