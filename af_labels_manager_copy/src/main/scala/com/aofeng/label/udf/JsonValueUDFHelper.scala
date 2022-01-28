package com.aofeng.label.udf

import com.aofeng.label.reader.{Label, MatchRule, Reader}
import com.aofeng.label.udf.matcher._

class JsonValueUDFHelper(labelId: String, labelReader: Reader[String, List[Label]], jsonReader: Reader[String, Map[String, MatchRule]]) extends UDFHelper {

  val subLabels: List[Label] = labelReader.read(labelId)
  // 计算所需要的所有字段名
  // 并且传入UDF的列值顺序也和Columns的顺序保持一致
  val columns: List[String] = subLabels.map(label => jsonReader.read(label.value)).reduce(_ ++ _).keys.toList

  override def createUDF(): String => String = {
    // 标签 -> 标签所有的计算条件
    // List[(Label, Map[Field, MatchRule])]
    val labelMatcherMap = subLabels.map(label => (label, jsonReader.read(label.value)))

    def udfFunction(fields: String): String = {
      val fieldValues = fields.split(",")

      // 循环所有Label, 找到满足条件的Label
      for ((label, matcherMap) <- labelMatcherMap) {

        // 循环单个Label的所有条件, 如果都满足, 则打上此标签
        val isMatches = matcherMap.toList
          .map(fieldAndMatchRule => (fieldValues(columns.indexOf(fieldAndMatchRule._1)), createMatcher(fieldAndMatchRule._2)))
          .map(fieldValueAndMatcher => fieldValueAndMatcher._2.matches(fieldValueAndMatcher._1))
          .reduce((x, y) => x && y)

        if (isMatches) {
          return label.id
        }
      }

      null
    }

    udfFunction
  }

  override def getColumns: List[String] = {
    columns
  }

  def createMatcher(matchRule: MatchRule): Matcher = {
    MatchType.fromName(matchRule.`type`) match {
      case MatchType.SINGLE => new SingleValueMatcher(matchRule.value)
      case MatchType.LIST => new ListMatcher(matchRule.value)
      case MatchType.RANGE => new RangeMatcher(matchRule.value)
      case _ => null
    }
  }
}