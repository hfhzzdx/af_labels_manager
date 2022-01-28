package com.aofeng.label.utils

import java.util.Properties
import java.util.regex.Pattern
import com.aofeng.label.constant.Constants
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

class SqlParseUtil {

  val REPLACER_PATTERN: Pattern = Pattern.compile("(\\{\\w+\\})")
  val properties: PropertiesUtil = new PropertiesUtil

  def getJobSqlList(appName: String, paramMap: Map[String, String], sqlName: String): List[String] = {
    val sqlConfig: Properties = properties.getJobConfigByName(appName,Constants.JOB_CONF_SERVICE_PATH)
    val sqlList = ListBuffer[String]()
    if (sqlConfig.containsKey("sqlOrder")) {
      for (s <- sqlConfig.getProperty("sqlOrder").split(",")) {
        sqlList += parserSql(sqlConfig.getProperty(s), paramMap)
      }
    } else {
      sqlList += parserSql(sqlConfig.getProperty(sqlName), paramMap)
    }
    sqlList.toList
  }

  def getJobSql(appName: String, paramMap: Map[String, String], sqlName: String): String = {
    val sqlConfig: Properties = properties.getJobConfigByName(appName,Constants.JOB_CONF_SERVICE_PATH)
    var marginSql = sqlConfig.getProperty(sqlName)
    if (StringUtils.isNotEmpty(marginSql) && !marginSql.equals("None")) {
      marginSql = parserSql(marginSql, paramMap)
    }
    marginSql
  }

  def parserSql(sql: String, paramMap: Map[String, String]): String = {
    var replacers: Set[String] = Set[String]()
    val matcher = REPLACER_PATTERN.matcher(sql)
    var finalSql = sql
    while (matcher.find) {
      val group = matcher.group
      if (!group.isEmpty) {
        replacers += group.substring(group.indexOf('{') + 1, group.indexOf('}'))
        //        println(replacers)
      }
    }
    for (x <- replacers) {
      finalSql = finalSql.replaceAll("\\{" + x + "\\}", paramMap(x).toString)
      //      println(sql)
    }
    finalSql
  }

}
