package com.aofeng.label.job

import java.util.Properties

import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.{JobTable, TableReader}
import com.aofeng.label.utils.{DateUtils, JdbcUtils, PropertiesUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import com.aofeng.label.constant.Constants
import com.aofeng.label.job.SaveToESWithMarketsJob

object GetResidentAttributes {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //需要一个udf,传入hive表的常驻字段.根据值返回改字段值对应的标签id

    val jobContext: TableJobContext = TableJobContext.create()
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "GetResidentAttributes"
    val job_desc = "河南移动精准营销2.0,读取hive表数据,生成用户基于基站的数据"
    var job_period = ""
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"
    val prop: Properties = (new PropertiesUtil).getPropertiesByJobName("GetResidentAttributesJob", Constants.JOB_CONF_SERVICE_PATH)

    //先读表hiveTable
    val source_table: String = prop.getProperty("sourceTableName")
    val lac_ci: String = prop.getProperty("lac_ci")
    val dest_table_name: String = prop.getProperty("destTableName")
    logger.info("GetResidentAttributesJob Support parameters usage: spark-submit --class  com.aofeng.label.job.GetResidentAttributesJob  [month_id].\r\n")
    println("GetResidentAttributesJob Support parameters usage: spark-submit --class  com.aofeng.label.job.GetResidentAttributesJob  [month_id].\r\n")
    if (args.length == 0) {
      job_period = DateUtils.getLastMonthId()
    } else {
      job_period = args(0)
    }
    JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)
    try {
      val jobTables = new TableReader(jobContext.jobConfig.loadJobConfig().getMetaStore).readSingle(source_table)
      //执行打标签操作
      System.err.println("GetResidentAttributesJob jobTables is " + jobTables.mkString(","))
      val jobTable = jobTables(0)
      val source: DataFrame = jobContext.readTable(jobTable.sourceName, jobTable.partitionField, job_period)

      logger.info("GetResidentAttributesJob read hive table is done. table name is " + source_table + " partition is " + job_period)
      val idColumn: Column = (new Column(jobContext.jobConfig.loadJobConfig().getIdColumn)) as Constants.COLUMN_ID
      val targetColumns = jobContext.processLabel(jobTable.pids.split("#").toList: _*) :+ idColumn :+ new Column(lac_ci)
      val df = source.select(targetColumns: _*)

      val columns: Array[Column] = df.columns.reverse.filter(!_.contains(Constants.COLUMN_ID)).map(c => new Column(c))

      val resultDF: DataFrame = df.select(new Column(Constants.COLUMN_ID), org.apache.spark.sql.functions.concat_ws("_", columns: _*) as Constants.COLUMN_LABELS)


      //调用编写好的行转列方法
      val frame: DataFrame = SaveToESWithMarketsJob.collectSet(jobContext, resultDF, ",", Constants.COLUMN_ID, Constants.COLUMN_LABELS, logger)
      //    frame.select("*").orderBy(new Column("id")).show(false)
      //写入hive表
      jobContext.saveToHive(frame, dest_table_name, job_period)
      JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
    } catch {
      case e: Exception => logger.error("GetResidentAttributesJob is failed. The message is " + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage + s"\r\n\t" + e.getStackTraceString)
        e.printStackTrace()
    }

  }

}
