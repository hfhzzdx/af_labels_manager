package com.aofeng.label.job

import java.util.Properties
import java.util.concurrent.{Callable, Executors}

import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.helper.YamlHelper
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.{JobTable, TableReader}
import com.aofeng.label.utils.{DateUtils, JdbcUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object BaseLabelsJob {
  private val logger: Logger = Logger.getLogger(this.getClass)
  val yaml: YamlHelper = new YamlHelper
  var APP_NAME: String = _
  var idColumn: Column = _
  var dataFrames: Map[String, DataFrame] = Map()
  var jobConfig: AFConfig = _
  var tableOrder: List[String] = _
  var jobTables: List[JobTable] = _
  var mainJoinTable: String = _
  var mysqlAttribute: Map[String, String] = Map()
  var schedulesList: ListBuffer[String] = ListBuffer()
  var end_time: String = ""
  var status: String = ""
  var res: String = ""
  val prop: Properties = new Properties()
  var url: String = _
  var tbl_schedules: String = _
  var job_desc: String = _
  var job_id: String = _
  var job_name: String = _
  var job_period: String = _
  var project: String = _
  var create_time: String = _
  var dataTime: String = _
  var table_name: String = _
  var jobContext: TableJobContext = _

  def init(): Unit = {
    logger.info("## BaseLabelsJob init start.")
    jobConfig = yaml.loadYamlFromConfig(Constants.JOB_CONF_PATH, classOf[AFConfig]).asInstanceOf[AFConfig]
    APP_NAME = jobConfig.getSpark.getAppName
    tableOrder = jobConfig.getTableOrder.asScala.toList
    mainJoinTable = jobConfig.getMainJoinTable
    table_name = jobConfig.getHive.getTable
    idColumn = new Column(jobConfig.getIdColumn)
    jobTables = new TableReader(jobConfig.getMetaStore).read("tbl_labels")
    project = "hn_jzyx"
    dataTime = jobConfig.getDataTime
    job_period = dataTime
    if (StringUtils.isEmpty(dataTime) || StringUtils.isBlank(dataTime)) {
      job_period = DateUtils.getLastMonthId()
    }
    mysqlAttribute += ("url" -> jobConfig.getMetaStore.getUrl,
      "driver" -> jobConfig.getMetaStore.getDriver,
      "user" -> jobConfig.getMetaStore.getUser,
      "password" -> jobConfig.getMetaStore.getPassword,
      "tbl_schedules" -> jobConfig.getMetaStore.getTbl_schedules
    )
    logger.info("## BaseLabelsJob init end. APP_NAME=" + APP_NAME + "\ttableOrder=" + tableOrder +
      "\tmainJoinTable=" + mainJoinTable + "\tidColumn=" + idColumn + "\tjobTables=" + jobTables
      + "\tmysqlAttribute=" + mysqlAttribute.mkString(","))

  }

  def main(args: Array[String]): Unit = {
    jobContext = TableJobContext.create()
    try {
      //1,初始化参数
      init()
      val startTime: Long = System.currentTimeMillis()
      logger.info(APP_NAME + " job start at time:" + DateUtils.getFormatDate19Now())
      job_id = jobContext.sqlContext.sparkContext.applicationId
      //2,获取需要处理的JobTable
      jobTables = screenJob(jobTables, tableOrder)
      logger.info("Get all need process jobTable:" + jobTables)
      //3,多线程并发提交spark job
      multiSubmitJob()
      try {
        //4,对结果进行汇总Join
        job_name = table_name;
        logger.info(job_name + " job start at time:" + DateUtils.getFormatDate19Now())
        job_desc = "河南移动精准营销项目,生成最终结果表数据,每月更新"
        val labelDF = joinAllDataFrame()
        //插入任务开始日志
        JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project,
          DateUtils.getFormatDate19Now(), "0", "start")
        jobContext.saveToHive(labelDF, table_name, job_period)
        //更新日志状态
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
      } catch {
        case e: Exception => logger.error(job_name + " job exec error:" + e.getMessage)
          JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+"\r\n"+e.getStackTraceString)
          e.printStackTrace()
      }
      logger.info(APP_NAME + " job end at time:" + DateUtils.getFormatDate19Now() +
        ",costTime:" + (System.currentTimeMillis() - startTime) / 1000)
    }catch {
      case e: Exception => logger.error(APP_NAME + " job exec error:" + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, dataTime, DateUtils.getFormatDate19Now(), "2", e.getMessage+" \r\n"+e.getStackTraceString)
        e.printStackTrace()
    }
    finally {
      jobContext.close()
    }
  }

  def multiSubmitJob(): Unit = {
    logger.info("multiSubmitJob execute start.")
    val jobExecutor = Executors.newFixedThreadPool(jobTables.length)
    val startTime: Long = System.currentTimeMillis()
    try {
      for (job <- jobTables) {
        //封装任务
        val result = jobExecutor.submit(new Callable[Map[String, DataFrame]] {
          override def call(): Map[String, DataFrame] = {
            try {
              job_name = job.sourceName
              job_desc = s"读取HIVE数据库${job.sourceName}数据,每月更新"
              val subStartTime: Long = System.currentTimeMillis()
              logger.info(job_name + " job start at time:" + DateUtils.getFormatDate19Now())
              //插入任务开始日志
              JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project,
                DateUtils.getFormatDate19Now(), "0", "start")
              //运行sql及UDF
              var source: DataFrame = jobContext.sqlContext.emptyDataFrame
              source = jobContext.readTable(job.sourceName, job.partitionField, job_period)
              val labelIds: List[String] = job.pids.split("#").toList
              var targetColumns = jobContext.processLabel(labelIds: _*)
              val idColumns = idColumn as Constants.COLUMN_ID
              targetColumns = targetColumns :+ idColumns
              logger.info("Get jobTable: " + job + ", targetColumns:" + targetColumns)
              logger.info("Submit read job by table:" + job.sourceName + ",partition:" + job.partitionField +
                ", subStartTime:" + DateUtils.getFormatDate19Now())
              val df = source.select(targetColumns: _*).drop(idColumn)
              //返回结果
              logger.info("End read job by table:" + job.sourceName +
                ",endTime:" + DateUtils.getFormatDate19Now() + ", costTime:" + (System.currentTimeMillis() - subStartTime) / 1000 + "s")
              dataFrames += (job.sourceName -> df)
              //更新日志状态
              JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
              logger.info(job_name + " job complete.costTime:" + (System.currentTimeMillis() - subStartTime) / 1000)
            } catch {
              case e: Exception => logger.error("Multithreading job is error:" + e.getMessage)
                JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+" \r\n "+e.getStackTraceString)
            }
            dataFrames
          }
        })
        logger.info("multiSubmitJob sumit complete.costTime:" + ((System.currentTimeMillis() - startTime) / 1000) +
          " result.size:" + result.get().keySet.size)
      }
    } catch {
      case e: Exception => logger.error("Multithreading job is error:" + e.getMessage)
        e.printStackTrace()
    }
    finally {
      jobExecutor.shutdown()
    }
    //      logger.info("多线程执行完毕!,总提交线程个数为" + jobExecutor.##)
    //      jobExecutor.wait()
  }

  def joinAllDataFrame(): DataFrame = {
    //先获取join主表
    var joinMainDF = dataFrames(mainJoinTable)
    logger.info("Get join main table:" + mainJoinTable)
    logger.info("BaseLabelsJob joinAllDataFrame dataFrames=" + dataFrames)
    dataFrames -= mainJoinTable
    //依次join所有DF
    for ((name, df) <- dataFrames) {
      logger.info("join table " + name)
      val frame: DataFrame = joinMainDF.join(df, Seq(Constants.COLUMN_ID), "left").drop(df.col(Constants.COLUMN_ID))
      logger.info("after join table " + name + ", joinMainDF columns:" + frame)
      joinMainDF = frame
    }
    //拼接输出字段 id,labels
    val columns = joinMainDF.columns.filter(!_.contains(Constants.COLUMN_ID)).map(l => new Column(l))
    //      columns.filter(!_.contains(Constants.COLUMN_ID))
    import org.apache.spark.sql.functions._
    val df: DataFrame = joinMainDF.select(new Column(Constants.COLUMN_ID), concat_ws(",", columns: _*) as Constants.COLUMN_LABELS)
    //    df.filter("id = 0").show(false)
    //    val l: Long = df.count()
    //    logger.info("total DataFrame's size is:"+l)
    df
  }


  def screenJob(job: List[JobTable], tableOrder: List[String]): List[JobTable] = {
    import scala.collection.mutable._
    var buffer = new ListBuffer[JobTable]
    for (e <- job) {
      if (tableOrder.contains(e.sourceName)) buffer += e
    }
    buffer.toList
  }

}
