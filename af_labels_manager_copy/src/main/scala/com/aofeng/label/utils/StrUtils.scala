package com.aofeng.label.utils

import com.aofeng.label.constant.Constants

object StrUtils {
  def toSeq(str: String): Seq[String] = {
    val strings: Array[String] = str.split(",")
    strings.toSeq
  }

  def toList(str: String): List[String] = {
    val strings: Array[String] = str.split(",")
    strings.toList
  }

  def splice(str: String, prefix: String = Constants.PREFIX_LABEL_COL): String = {
    var string: String = ""
    val array: Array[String] = str.split(",")
    val strArr: Array[String] = array.map((ss: String) => prefix + ss)
    for (ss <- strArr) {
      string += ss + ','
    }
    string.substring(0, string.length - 1)
  }

  def getFieldsWithLabel(fields: String): String = {
    var string: String = " as "
    var s = ""
    val strings = fields.split(",")
    val array = strings.map((ss: String) => "\'" + ss + "\'" + string + Constants.PREFIX_LABEL_COL + ss)
    for (str <- array) {
      string += str + ','
    }
    string.substring(3, string.length - 1)
  }

  def getConfigMapping(str: String, separator: Char = ',', character: Char = ':'): Map[String, String] = {
    val arr: Array[String] = str.split(separator)
    val array: Array[Array[String]] = arr.map(ss => ss.split(character))
    val map: Map[String, String] = array.map { case Array(x, y) => (x, y) }.toMap
    map
  }

  def getColumn(col: String) = {
    var s = ""
    val strings: Array[String] = col.split(",")
    val array: Array[String] = strings.map(rr => "\"" + rr + "\"")
    for (str <- array) {
      s += str + ','
    }
    s.substring(0, s.length - 1)
  }

  def listToString(list: List[String]) = {
    var str: String = ""
    val leg = list.length
    for (ll <- list) {
      var str1: String = ll.toString.concat(",")
      str += str1
    }
    str.substring(0, str.length - 1)
  }

  def StrToListBuffer(str: String) = {
    import scala.collection.mutable.ListBuffer
    val list: List[String] = str.replaceAll('\"'.toString, "").replaceAll(" ".toString, "").split(",").toList
    val strings: ListBuffer[String] = list.to[ListBuffer]
    strings
  }

  def buffToString(list: List[Int]): String = {
    var str: String = ""
    for (ll <- list) {
      var str1: String = ll.toString.concat(",")
      str += str1
    }
    str.substring(0, str.length - 1)
  }

}

