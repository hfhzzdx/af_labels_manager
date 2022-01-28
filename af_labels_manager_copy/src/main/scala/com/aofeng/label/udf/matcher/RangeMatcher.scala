package com.aofeng.label.udf.matcher

import com.google.common.collect.{Range, Ranges}

class RangeMatcher(value: String) extends Matcher {
  //  private val logger: Logger = Logger.getLogger(this.getClass)
  override def matches(otherValue: String): Boolean = {
    //(0.3,0.7]
    val leftType = value.charAt(0)
    val leftNum = value.substring(1, value.indexOf(',')).toDouble //0.3
    val rightType = value.charAt(value.length - 1)
    val rightNum = value.substring(value.indexOf(',') + 1, value.length - 1).toDouble

    var leftRange: Range[java.lang.Double] = null
    if ('[' equals leftType) {
      leftRange = Ranges.atLeast(leftNum) //(0.3,x)
    }
    if ('(' equals leftType) {
      leftRange = Ranges.greaterThan(leftNum)
    }

    var rightRange: Range[java.lang.Double] = null
    if (']' equals rightType) {
      rightRange = Ranges.atMost(rightNum)
    }
    if (')' equals rightType) {
      rightRange = Ranges.lessThan(rightNum)
    }

    val range = leftRange intersection rightRange
    //logger.fatal("UDFRanger left Range is "+leftRange+",rightRange is "+rightRange+",result range is "+range+",otherValue is "+otherValue)
    range.contains(otherValue.toDouble)
  }
}
