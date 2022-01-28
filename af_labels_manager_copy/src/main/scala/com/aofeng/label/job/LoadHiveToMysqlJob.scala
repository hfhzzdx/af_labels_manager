package com.aofeng.label.job


import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.utils.{DateUtils, JdbcUtils, PropertiesUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer


object LoadHiveToMysqlJob {
  private val logger: Logger = Logger.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    //读取mysql表(因为河南环境hadoop集群的所有节点不和mysql节点互通,故只能使用local模式)
    //读取mysql   month_id为上个月
    //先判断参数个数

    val jobContext: TableJobContext = TableJobContext.create()
    //    val logger=jobContext.logger
    //    var connection:java.sql.Connection = _

    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "LoadHiveToMysqlJob"
    val job_desc = "河南移动精准营销2.0,读取HIVE数据,同步至Mysql"
    var job_period = ""
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"
    var batchSize = 0
    val prop: Properties = (new PropertiesUtil).getPropertiesByJobName("LoadHiveToMysqlJob", Constants.JOB_CONF_SERVICE_PATH)
    if (args.length < 2) {
      logger.error("args are wrong. fg: spark-submit --class com.aofeng.label.job.LoadHiveToMysqlJob hiveDB.table mysqlDb.table [month_id] ")
      System.err.println("args are wrong. fg: spark-submit --class com.aofeng.label.job.LoadHiveToMysqlJob hiveDB.table mysqlDb.table [month_id]")
//      JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "2", "parameter error!!! usage:  spark-submit --class com.aofeng.label.job.LoadHiveToMysqlJob hiveDB.table mysqlDb.table [month_id]")
      System.exit(0)
    } else {
      val driver: String = prop.getProperty("mysqlDriver")
      val mysqlUrl: String = prop.getProperty("mysqlUrl")
      val user: String = prop.getProperty("mysqlUser")
      val password: String = prop.getProperty("mysqlPassword")
      val batchSize:String = prop.getProperty("batchSize")
      var url: String = ""
      try {
        if (args(1).contains(".")) {
          val mysqlDb = args(1).split("\\.")(0)
          url = mysqlUrl.replace("3306/?", s"3306/${mysqlDb}?")
        }
        else {
//          JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "2", "parameter error!!!   args(1) is mysqlDb.tableName which is not only include mysqlDb or tableName")
          logger.error("LoadHiveToMysqlJob failed .parameter error!!!  args(1) is mysqlDb.tableName which is not only include mysqlDb or tableName")
          System.err.println("LoadHiveToMysqlJob failed .parameter error!!!  args(1) is mysqlDb.tableName which is not only include mysqlDb or tableName")
          System.exit(0)
        }
      } catch {
        case e: Exception => logger.error("LoadHiveToMysqlJob is failed. the message is " + e.getMessage)
          JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "2", e.toString + "\t\r\n\t" + e.getStackTraceString)
          e.printStackTrace()
          System.exit(0)
      }
      logger.info("args length=" + args.length)
      var month_id = ""
      val mysqlTable = args(1)
      var sql = s"delete from  ${mysqlTable} where month_id = "
      val connection: Connection = JdbcUtils.getConnection(url, user, password, driver)
      val statement: Statement = connection.createStatement()
      try {
        if (args.length == 2) {
          month_id = DateUtils.getLastMonthId()
          job_period = month_id
          val str: String = DateUtils.getLastMonth(month_id, 2)
          sql += s"\'${str}\'"
          JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "0", "start")
          val rows: Int = statement.executeUpdate(sql)
          logger.info("LoadHiveToMysqlJob delete sql is done, number of rows affected are " + rows + " sql=" + sql)
          logger.info("Prepare to Save data to Mysql job. source table name is " + args(0) + " dst mysql table name is " + args(1) + " dateTime is " + month_id)
          val frame: DataFrame = jobContext.readHiveTable(args(0)).filter(s"month_id = ${month_id}")
          JdbcUtils.batchInsertToMysql(frame, jobContext, SaveMode.Append, driver, url, user, password, month_id, args(1),batchSize)
        } else if (args.length == 3) {
          month_id = args(2)
          job_period = month_id
          val str = DateUtils.getLastMonth(month_id, 2)
          sql = sql + s"\'${str}\'"
          JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "0", "start")
          val rows: Int = statement.executeUpdate(sql)
          logger.info("LoadHiveToMysqlJob delete sql is done. numbers of rows affected are " + rows + " sql=" + sql)
          logger.info("Prepare to Save data to Mysql job. source table name is " + args(0) + " dst mysql table name is " + args(1) + " dateTime is " + args(2))
          val frame: DataFrame = jobContext.readHiveTable(args(0)).filter(s"month_id=${month_id}")
//          JdbcUtils.saveToMysql(frame, jobContext, SaveMode.Append, driver, url, user, password, month_id, args(1))
          JdbcUtils.batchInsertToMysql(frame, jobContext, SaveMode.Append, driver, url, user, password, month_id, args(1),batchSize)
        }
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
      } catch {
        case e: Exception => logger.error("LoadHiveToMysqlJob is failed,the message is " + e.getMessage)
          JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage + "\r\n  " + e.getStackTraceString)
          e.printStackTrace()
      } finally {
        if (statement != null) {
          statement.close()
          if (connection != null) {
            connection.close()
          }
        }
        jobContext.close()
      }
    }
  }
}
