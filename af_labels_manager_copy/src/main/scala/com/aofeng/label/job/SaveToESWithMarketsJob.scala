package com.aofeng.label.job

import java.sql.{PreparedStatement, ResultSet}

import com.alibaba.druid.pool.DruidPooledConnection
import com.aofeng.label.config.AFConfig
import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.utils.{DateUtils, ESUtils, JdbcUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object SaveToESWithMarketsJob {
  private val logger: Logger = Logger.getLogger(this.getClass)
  //  private val jobConfig: AFConfig = BaseLabelsJob.yaml.loadYamlFromConfig(Constants.JOB_CONF_PATH, classOf[AFConfig]).asInstanceOf[AFConfig]

  def main(args: Array[String]): Unit = {
    /**
     * mysql> select * from test where unix_timestamp(create_time)>unix_timestamp('20200711');
     * +----+---------------------+------------+---------------------+
     * | id | create_time         | end_time   | test_time           |
     * +----+---------------------+------------+---------------------+
     * |  1 | 2021-08-05 15:00:00 | 2021-08-05 | 2021-08-05 15:00:00 |
     * |  2 | 2021-07-01 15:00:00 | 2021-07-01 | 2021-07-01 15:00:00 |
     * +----+---------------------+------------+---------------------+
     * 2 rows in set (0.07 sec)
     * mysql> select * from test where unix_timestamp(create_time)>unix_timestamp('20210711');
     * +----+---------------------+------------+---------------------+
     * | id | create_time         | end_time   | test_time           |
     * +----+---------------------+------------+---------------------+
     * |  1 | 2021-08-05 15:00:00 | 2021-08-05 | 2021-08-05 15:00:00 |
     * +----+---------------------+------------+---------------------+
     * 1 row in set (0.06 sec)
     */
    //读取mysql数据
    //先做group_concat进行拼接,由之前的批次id对应userIds转换成活动id对应userIds
    val jobContext: TableJobContext = TableJobContext.create()
    //    val logger = jobContext.logger
    val jobConfig: AFConfig = jobContext.jobConfig.loadJobConfig()
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "SaveToESWithMarketsJob"
    val job_desc = "河南移动精准营销,将结果数据同步至ES"
    var job_period = ""
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"


    val dataTime: String = jobConfig.getDataTime
    val jobStartTime: Long = System.currentTimeMillis()
    val atid: String = jobConfig.getMetaStore.getAtid
    val userids: String = jobConfig.getMetaStore.getUserids
    val tbl_batch_user: String = jobConfig.getMetaStore.getTbl_batch_user
    val tbl_actions: String = jobConfig.getMetaStore.getTbl_actions
    val n: String = jobConfig.getMetaStore.getMonthAgo
    val bstatus: String = jobConfig.getMetaStore.getBstatus
    var sql = ""
    var month_id: String = ""
    logger.info("SaveToESWithMarketsJob is starting, get all config is done")
    if (StringUtils.isBlank(dataTime) || StringUtils.isEmpty(dataTime)) {
      if (StringUtils.isEmpty(n) || StringUtils.isBlank(n)) {
        sql = s"select distinct a.atid,a.userids from ${tbl_batch_user} as a inner join " +
          s"(select atid from ${tbl_actions} where ${bstatus}='NOTERESSUC') as b" +
          s" on a.atid=b.atid"
      } else {
        val month: String = DateUtils.getNMonthAgo(n.toInt)
        sql = s"select distinct a.atid,a.userids from ${tbl_batch_user} as a inner join " +
          s"(select atid from ${tbl_actions} where unix_timestamp(send_time) >= unix_timestamp(${month}) and ${bstatus}='NOTERESSUC') as b" +
          s" on a.atid=b.atid"
      }
      month_id = DateUtils.getLastMonthId()
      job_period = month_id
    } else {
      var month = ""
      if (StringUtils.isEmpty(n) || StringUtils.isBlank(n)) {
        month = DateUtils.getLastMonth(dataTime, 6)
      } else {
        month = DateUtils.getLastMonth(dataTime, n.toInt)
      }
      month_id = dataTime
      job_period = month_id
      sql = s"select distinct a.atid,a.userids from ${tbl_batch_user} as a  inner join " +
        s"(select atid from ${tbl_actions} where  unix_timestamp(send_time) >= unix_timestamp(${month}) and ${bstatus}='NOTERESSUC') as b" +
        s" on a.atid=b.atid"
    }
    JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)
    try {
      //运行sql获取ResultSet
      logger.info("SaveToESWithMarketsJob get mysql.sql=" + sql)
      val resultSet: ResultSet = JdbcUtils.execSql(sql)
      //ResultSet to DataFrame
      logger.info("SaveToESWithMarketsJob get mysql ResultSet is done! Prepare for data conversion ...")
      val frame: DataFrame = JdbcUtils.createResultSetToDF(resultSet, jobContext)
      logger.info("SaveToESWithMarketsJob ResultSet to DataFrame method is all done. Prepare for data conversion run collect_set ...")
      val mysqlDF: DataFrame = collectSet(jobContext, frame, ",", userids, atid, logger)
      logger.info(" SaveToESWithMarketsJob run custom functions is done. Total costTime is " + (System.currentTimeMillis() - jobStartTime) / 1000 + "s")

      //获取hive数据库数据
      var hiveDF = jobContext.sqlContext.emptyDataFrame
      val partitionField: String = jobConfig.getHive.getPartitionField
      logger.info("SaveToESWithMarketsJob load hive table is done, load table name is " + jobConfig.getHive.getTable + " dataTime is " + month_id)
      hiveDF = jobContext.readHiveTable(jobConfig.getHive.getTable).filter(s"${partitionField}=${month_id}")


      //mysql和hive进行join
      val idColumn: String = jobConfig.getHive.getIdColumn
      logger.info("SaveToESWithMarketsJob Prepare to hiveDF left join mysqlDF")
      val dataFrame: DataFrame = hiveDF.join(mysqlDF, hiveDF.col(idColumn) === mysqlDF.col(userids), "left").drop(mysqlDF.col(userids)).drop(hiveDF.col("month_id"))
      logger.info("SaveToESWithMarketsJob DataFrame join job is done. Total costTime is " + (System.currentTimeMillis() - jobStartTime) / 1000 + " s")

      JdbcUtils.connectionClose(resultSet)

      //dataFrame 字段重命名
      val resultDF: DataFrame = dataFrame.withColumnRenamed("atid", "markets")
      //准备数据入ES
      logger.info("SaveToESWithMarketsJob Prepare to load Data to ElasticSearch .")
      ESUtils.saveToES(resultDF, jobConfig.getEs.getIndex, jobConfig.getEs.getNodes, jobConfig.getEs.getPort, jobConfig.getHive.getIdColumn, jobContext)
      logger.info("SaveToESWithMarketsJob all job is done. Total costTime is " + (System.currentTimeMillis() - jobStartTime) / 1000 + " s")

      JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")

    } catch {
      case e: Exception => logger.error("SaveToESWithMarketsJob is failed,the message is " + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+"\r\n\t"+e.getStackTraceString)
    } finally {

      jobContext.close()
    }
  }

  /**
   * 自定义collect_set函数
   *
   * @param jobContext jobContext对象
   * @param df         dataFrame对象
   * @param separate   原字段分隔符
   * @param fieldName1 需要以指定分隔符切割的字段  userids
   * @param fieldName2 需要进行聚合的字段 markets
   * @return 返回分组聚合并去重后的dataFrame
   *         collectSet(jobContext,frame,",","userids","markets")
   *         例子: 源表     frame
   *         +-------+-----------------------------+
   *         |markets|userids                      |
   *         +-------+-----------------------------+
   *         |102    |33,44,55,66,77,33,77,66,88,99|
   *         |105    |22,44                        |
   *         |103    |11,22,33,44                  |
   *         +-------+-----------------------------+
   *         返回    collectSet(jobContext,frame,",","userids","markets")
   *         +-------+-----------+
   *         |userids|markets    |
   *         +-------+-----------+
   *         |22     |105,103    |
   *         |99     |102        |
   *         |33     |102,103    |
   *         |44     |102,105,103|
   *         |55     |102        |
   *         |66     |102        |
   *         |77     |102        |
   *         |11     |103        |
   *         |88     |102        |
   *         +-------+-----------+
   */
  def collectSet(jobContext: TableJobContext, df: DataFrame, separate: String, fieldName1: String, fieldName2: String, logger: Logger): DataFrame = {
    import org.apache.spark.sql.functions
    import com.aofeng.label.reader.UserMarkets
    val sqlContext: SQLContext = jobContext.sqlContext
    val start = System.currentTimeMillis()
    import sqlContext.implicits._
    logger.info("SaveToESWithMarketsJob collect_set is preparing to start...")
    logger.info("SaveToESWithMarketsJob collect_set df.withColumnRenamed old fieldName is " + fieldName1 + " newName is fieldName1")
    val user_id_atid: DataFrame = df.withColumnRenamed(fieldName1, "fieldName1")
      .withColumnRenamed(fieldName2, "fieldName2")
      .withColumn("col_1", functions.explode(functions.split(functions.col("fieldName1"), separate)))
    val ds: Dataset[UserMarkets] = user_id_atid.as[UserMarkets]
    val demo: RDD[(String, UserMarkets)] = ds.map(r => ((r.col_1), r)).rdd
    val seqOp = (a: List[String], b: UserMarkets) => (b.fieldName2 :: a).distinct
    val combOp = (a: List[String], b: List[String]) => {
      (a ::: b).distinct
    }
    logger.info("SaveToESWithMarketsJob collect_set prepare distinct.")
    val rdd: RDD[(String, String)] = demo.aggregateByKey(List[String]())(seqOp, combOp).mapValues(ll => ll.mkString(","))
    val schemaString: String = fieldName1.concat(",").concat(fieldName2)
    val fields = schemaString.split(",").map(fileName => StructField(fileName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD: RDD[Row] = rdd.map(att => Row(att._1, att._2))
    logger.info("SaveToESWithMarketsJob collect_set prepare create dataFrame.")
    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    logger.info("SaveToESWithMarketsJob collect_set is done . costTime is " + (System.currentTimeMillis() - start) / 1000 + " s")
    dataFrame
  }


}
