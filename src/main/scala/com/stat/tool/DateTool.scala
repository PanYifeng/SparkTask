package com.stat.tool

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

object DateTool {
  //获取今天日期
  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var nowDate = dateFormat.format(now)
    nowDate
  }
  def getNowDate(format: String): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    var nowDate = dateFormat.format(now)
    nowDate
  }

  //获取之前N天的日期
  def getYesterday(): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getYesterday(format: String): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getYesterday(format: String, n: Int): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -n)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  //获取本周开始日期
  def getNowWeekStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = df.format(cal.getTime())
    period
  }
  def getNowWeekStart(format: String): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat(format);
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = df.format(cal.getTime())
    period
  }

  //获取本周末的时间
  def getNowWeekEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime())
    period
  }
  def getNowWeekEnd(format: String): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat(format);
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime())
    period
  }

  //将时间戳转化成日期
  def dateFormat(time: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date: String = sdf.format(new Date((time.toLong)))
    date
  }
  def dateFormat(time: String, format: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat(format)
    var date: String = sdf.format(new Date((time.toLong)))
    date
  }
}