package com.aofeng.label.utils

import com.aofeng.label.constant.Constants

class ParameterUtils {

  var paramMap: Map[String, String] = Map()

  def getAllParams(): Map[String, String] = {
    paramMap += (Constants.PARAM_DT -> DateUtils.getDataPartition())
    paramMap += (Constants.PARAM_YEAR -> DateUtils.getYear())
    paramMap += (Constants.PARAM_LAST_MONTH_DAY -> DateUtils.getLastMonthDay())
    paramMap += (Constants.PARAM_LAST_MONTH_ID -> DateUtils.getLastMonthId())
    paramMap += (Constants.PARAM_DAY_ID -> DateUtils.getLastMonthDay())
    paramMap += (Constants.PARAM_MONTH_ID -> DateUtils.getLastMonthId())
    //    setDataPartition()
    //    setYear()
    //    setLastMonthDay()
    //    setLastMonthId()
    paramMap
  }

  //  def setDataPartition(): Unit = {
  //    paramMap += (Constants.PARAM_DT -> DateUtils.getDataPartition())
  //  }
  //
  //  def setYear(): Unit = {
  //    paramMap += (Constants.PARAM_YEAR -> DateUtils.getYear())
  //  }
  //
  //
  //  def setLastMonthDay() = {
  //    paramMap += (Constants.PARAM_LAST_MONTH_DAY -> DateUtils.getLastMonthDay())
  //  }
  //
  //  def setLastMonthId() = {
  //    paramMap += (Constants.PARAM_LAST_MONTH_ID -> DateUtils.getLastMonthId())
  //  }

}
