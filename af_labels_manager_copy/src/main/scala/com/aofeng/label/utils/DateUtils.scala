package com.aofeng.label.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {
  val DATE_FORMAT_YYYYMM: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
  val DATE_FORMAT_YYYYMMDD: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val DATE_FORMAT_19: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT_14: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  def getFormatDate19(time: Object): String = {
    DATE_FORMAT_19.format(time)
  }

  def getFormatDate14(time: Object): String = {
    DATE_FORMAT_14.format(time)
  }

  def getFormatDate19Now(): String = {
    DATE_FORMAT_19.format(new Date())
  }

  def getFormatDate14Now(): String = {
    DATE_FORMAT_14.format(new Date())
  }

  def getDataPartition(): String = {
    DATE_FORMAT_YYYYMM.format(new Date())
  }

  def getYear(): String = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.YEAR, -1)
    val y = cal.getTime
    DATE_FORMAT_YYYYMM.format(y)
  }


  def getLastMonthDay() = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.DATE, 0)
    DATE_FORMAT_YYYYMMDD.format(cal.getTime())
  }

  def getLastMonthId() = {
    val cal: Calendar = Calendar.getInstance()
    cal.set(Calendar.DATE, 0)
    DATE_FORMAT_YYYYMM.format(cal.getTime())
  }

  def getNextMonth(month_id: String): String = {
    val c = getCalendar(DATE_FORMAT_YYYYMM.parse(month_id))
    c.add(Calendar.MONTH, 1)
    DATE_FORMAT_YYYYMM.format(c.getTime)
  }

  def getCalendar(day: Date): Calendar = {
    val c = Calendar.getInstance
    c.clear
    if (day != null) c.setTime(day)
    c
  }

  def getNMonthAgo(n: Int) = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.MONTH, -n)
    val time: Date = cal.getTime
    val str: String = DATE_FORMAT_YYYYMMDD.format(time)
    str
  }

  def getTongBiDate(date1: String): (String, String, String, String, String) = {
    val dft = new SimpleDateFormat("yyyyMMdd")
    val dft1 = new SimpleDateFormat("dd")
    val dft2 = new SimpleDateFormat("yyyyMM")

    val c = getCalendar(dft.parse(date1))
    c.set(Calendar.HOUR_OF_DAY, -24)
    val date = dft.format(c.getTime)
    // 今天日期
    val day: String = dft1.format(c.getTime)
    val today: Int = day.toInt
    val ret3 = dft2.format(c.getTime)
    // 昨天
    c.set(Calendar.HOUR_OF_DAY, -24)
    val ret4 = dft.format(c.getTime)
    c.set(Calendar.HOUR_OF_DAY, 24)
    // 上一个月
    c.add(Calendar.MONTH, -1)
    val ret1 = dft2.format(c.getTime)
    // 最大天数
    val maxDay: Int = c.getActualMaximum(Calendar.DAY_OF_MONTH)
    var ret: String = null
    if (today <= maxDay) {
      ret = ret1 + day
    } else {
      c.set(Calendar.DAY_OF_MONTH, maxDay)
      ret = dft.format(c.getTime)
    }
    // 今天，今天月份，昨天，同比月份，同比天
    (date, ret3, ret4, ret1, ret)

  }
def getLastMonth(month_id:String,n:Int):String={
  val date: Date = DATE_FORMAT_YYYYMM.parse(month_id)
  val c=getCalendar(date)
  c.add(Calendar.MONTH, -n)
  DATE_FORMAT_YYYYMM.format(c.getTime)
}

  def main(args: Array[String]): Unit = {
    println(getLastMonthId())
    println(getLastMonthDay())
    println(getNextMonth("202106"))
    println(getLastMonth("202106",12))
    println(getNMonthAgo(12))
    println(getYear())
  }


}



