package com.aofeng.label.job

import java.util.Properties

import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.{JobTable, TableReader}
import com.aofeng.label.utils.{DateUtils, ESUtils, JdbcUtils, PropertiesUtil, StrUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

object UpdateESJob {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val jobContext: TableJobContext = TableJobContext.create()
    val propertiesUtil = new PropertiesUtil
    val prop: Properties = propertiesUtil.getPropertiesByJobName("UpdateESJob", Constants.JOB_CONF_SERVICE_PATH)
    //先获取参数个数
    val start=System.currentTimeMillis()
    var job_period = ""
    var index = ""
    var action = ""
    var nodes=""
    var port=""
    var idList=""
    if (args.length < 3) {
      logger.error("UpdateESJob Support parameters usage: spark-submit --class  com.aofeng.label.job.UpdateESJob jzyx.user_labels_finals user_labels/doc add|delete [id1] [month_id].\r\n" +
        "args(0): jzyx.user_labels_finals.  Which table is need to calculate labels\r\n" +
        "args(1): user_labels/doc. ES's indexName/type which must include type\r\n" +
        "args(2): add. How to operate,only support two operation: add or delete. IgnoreCase"+
        "" +
        "(3): if args(2) is add, args(3) is month_id. else if args(2) is delete ,args(3) is which id is needed to delete. args(4) is month_id")
      System.err.println("UpdateESJob Support parameters usage: spark-submit --class  com.aofeng.label.job.UpdateESJob jzyx.user_labels_finals user_labels/doc add|delete [id1] [month_id].\r\n" +
        "args(0): jzyx.user_labels_finals.  Which table is need to calculate labels\r\n" +
        "args(1): user_labels/doc. ES's indexName/type which must include type\r\n" +
        "args(2): add. How to operate,only support  two operation: add or delete. IgnoreCase"+
      "args(3): if args(2) is add, args(3) is month_id. else if args(2) is delete ,args(3) is which id is needed to delete. args(4) is month_id")
      System.exit(0)
    } else {
      index = args(1)
      action = args(2)
      nodes=prop.getProperty("es.nodes")
      port=prop.getProperty("es.port")

      if (!action.equalsIgnoreCase("delete") && !action.equalsIgnoreCase("add")){
        logger.error("UpdateESJob parameters error, action is wrong. only support delete or add. please check args(2) . \t\t\t System is quiting......")
        System.err.println("UpdateESJob parameters error, action is wrong. only support delete or add. please check args(2) . \t\t\t System is quiting......")
        System.exit(0)
      }
      else if (action.equalsIgnoreCase("add")){
        if(args.length == 4){
          job_period = args(3)
        }else {job_period =  DateUtils.getLastMonthId()}
      }
      else {
        if (args.length < 4){
          logger.error("UpdateESJob parameters error, please input which id is needed to delete args(3). \t\t\t System is quiting......")
          System.err.println("UpdateESJob parameters error, please input which id is needed to delete args(3). \t\t\t System is quiting......")
          System.exit(0)
        }else if(args.length == 4){
          idList =  args(3)
          job_period = DateUtils.getLastMonthId()
        }else {
          idList =  args(3)
          job_period = args(4)
        }
      }
    }
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "UpdateESJob"
    val job_desc = "河南移动精准营销2.0,读取HIVE数据,生成标签更新至ES"
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"
    val map: Map[String, String] = propertiesUtil.propToMap(prop).toMap
    //读取mysql表打标签
    val source_name = args(0)
    JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)
    try {
      logger.info("UpdateESJob is prepare to starting... All parameters got done.")
      val jobTables: List[JobTable] = new TableReader(jobContext.jobConfig.loadJobConfig().getMetaStore).readSingle(source_name)
      //执行打标签操作
      System.err.println("jobTables is " + jobTables.mkString(","))
      val jobTable = jobTables(0)
      val source: DataFrame = jobContext.readTable(jobTable.sourceName, jobTable.partitionField, job_period)
      logger.info("UpdateESJob read hive table is done. table name is "+source_name+" partition is "+ job_period)
      val idColumn: Column = (new Column(jobContext.jobConfig.loadJobConfig().getIdColumn)) as Constants.COLUMN_ID
      val targetColumns = jobContext.processLabel(jobTable.pids.split("#").toList: _*) :+ idColumn
      val df = source.select(targetColumns: _*).drop(idColumn)
      val columns: Array[Column] = df.columns.filter(!_.contains(Constants.COLUMN_ID)).map(c => new Column(c))
      val resultDF: DataFrame = df.select(new Column(Constants.COLUMN_ID), org.apache.spark.sql.functions.concat_ws(",", columns: _*) as Constants.COLUMN_LABELS)
      //调用es更新操作
      logger.info("UpdateESJob labels job is done. Total costTime is "+ (System.currentTimeMillis()-start))
      logger.info("UpdateESJob prepare to updateEs. current time is "+ DateUtils.getFormatDate19Now()+", current timestamp is "+System.currentTimeMillis())
      val startTime: Long = System.currentTimeMillis()

      ESUtils.updateES(jobContext,jobTable, resultDF,nodes,port, index, Constants.COLUMN_LABELS, action,map,idList)
      logger.info("UpdateESJob all job is done! Total costTime is "+(System.currentTimeMillis()-start)/1000+" s")
      logger.info("UpdateESJob all job is done! Total costTime is "+(System.currentTimeMillis()-start))
      JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
    } catch {
      case e: Exception => logger.error("UpdateESJob is failed. The message is " + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage + "\r\n\t" + e.getStackTraceString)
        e.printStackTrace()
    }

  }

}
