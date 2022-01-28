package com.aofeng.label.udf.matcher

class ListMatcher(value: String) extends Matcher {

  override def matches(otherValue: String): Boolean = {
    val valueList = value.split(",")
    valueList.contains(otherValue)
  }
}
