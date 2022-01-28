package com.aofeng.label.job

import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.helper.YamlHelper
import org.apache.spark.sql.{Column, DataFrame}
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.utils.{DateUtils, ESUtils, JdbcUtils, PropertiesUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{FileAppender, Logger}
import org.apache.spark.sql.functions.col
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer


object LoadHiveToESJob {

  private val logger: Logger = Logger.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    //    val month_id: String = DateUtils.getLastMonthId()
    //    val startTime: String = DateUtils.getFormatDate19Now()
    val jobContext = TableJobContext.create()

    //获取配置
    //    val logger = jobContext.logger
    val proper = new PropertiesUtil
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "LoadHiveToESJob"
    val job_desc = "河南移动精准营销2.0,读取HIVE数据,同步至ES"
    var job_period = ""
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"

    if (args.length < 1) {
      logger.error("LoadHiveToESJob Support parameters usage: spark-submit --class  com.aofeng.label.job.LoadHiveToESJob LoadHiveToESJob  [month_id] \r\n args(0):LoadHiveToESJob is properties name. properties file path is resource/jobConfig")
      System.err.println("LoadHiveToESJob Support parameters usage: spark-submit --class com.aofeng.label.job.LoadHiveToESJob LoadHiveToESJob [month_id] \r\n args(0):LoadHiveToESJob is properties name. properties file path is resource/jobConfig")
//      JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, "2", "parameter error!!! usage: spark-submit --class com.aofeng.label.job.LoadHiveToESJob LoadHiveToESJob [month_id] \r\n args(0):LoadHiveToESJob is properties name. properties file path is resource/jobConfig")
      System.exit(0)
    } else {
      val jobProper: Properties = proper.getPropertiesByJobName(args(0), Constants.JOB_CONF_SERVICE_PATH)
      val hive_table = jobProper.getProperty("tableName")
      val partitionField: String = jobProper.getProperty("tablePartitionField")
      val idColumn: String = jobProper.getProperty("tableIdColumn")
      val otherColumns: String = jobProper.getProperty("otherColumns")
      val es_index = jobProper.getProperty("es.index")
      val es_nodes = jobProper.getProperty("es.nodes")
      val es_port = jobProper.getProperty("es.port")
      val start = System.currentTimeMillis()
      var month_id = ""
      if (args.length == 1) {
          month_id = DateUtils.getLastMonthId()
          job_period = month_id
          logger.info("LoadHiveToESJob prepare to starting... current dataTime is " + month_id)
      } else {
        month_id = args(1)
        job_period = month_id
        logger.info("LoadHiveToESJob prepare to starting... current dataTime is " + month_id)
      }
      JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)
      try {
        if (StringUtils.isNotEmpty(otherColumns) || StringUtils.isNotBlank(otherColumns)) {
          val column_list: List[String] = otherColumns.split("#").toList
          val hiveDF: DataFrame = jobContext.readHiveTable(hive_table).filter(s"$partitionField=$month_id")
          val targetColumns: List[String] = idColumn +: column_list

          val df: DataFrame = hiveDF.select(targetColumns.map(name => col(name)): _*)
          logger.info("LoadHiveToEs job,get otherColumns is not null,otherColumns=" + otherColumns)
          ESUtils.saveToES(df, es_index, es_nodes, es_port, idColumn, jobContext)
          logger.info("LoadHiveToESJob is done. otherColumns is not null. total costTime is " + (System.currentTimeMillis() - start) / 1000 + " s")

        } else {
          val df: DataFrame = jobContext.readHiveTable(hive_table).filter(s"$partitionField=$month_id").select(idColumn)
          ESUtils.saveToES(df, es_index, es_nodes, es_port, idColumn, jobContext)
          logger.info("LoadHiveToESJob is done. otherColumns is null. total costTime is " + (System.currentTimeMillis() - start) / 1000 + " s")
        }

        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
      } catch {
        case e: Exception => logger.error("LoadHiveToESJob is failed,the message is " + e.getMessage)
          JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+"\r\n "+e.getStackTraceString)
          e.printStackTrace()
      } finally {
        jobContext.close()
      }

    }
  }
}
