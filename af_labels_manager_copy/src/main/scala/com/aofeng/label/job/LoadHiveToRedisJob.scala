package com.aofeng.label.job


import com.aofeng.label.module.TableJobContext
import com.aofeng.label.utils.{DateUtils, JdbcUtils, PropertiesUtil, RedisUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import com.aofeng.label.constant.Constants
import java.util.Properties


object LoadHiveToRedisJob extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val prop: Properties = (new PropertiesUtil).getPropertiesByJobName("LoadHiveToRedisJob", Constants.JOB_CONF_SERVICE_PATH)

    //    读取hive表数据
    val jobContext = TableJobContext.create()
    val beginTime: Long = System.currentTimeMillis()
    val tableName: String = prop.getProperty("tableName")
    val partition: String = prop.getProperty("tablePartitionField")
    val redis_key: String = prop.getProperty("key")
    val redis_value: String = prop.getProperty("value")
    val redis_index: String = prop.getProperty("index")
    val redis_type: String = prop.getProperty("type")
    logger.info("LoadHiveToRedis job is starting... read config is done")
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "LoadHiveToRedisJob"
    val job_desc = "河南移动精准营销2.0,读取HIVE数据,同步至Redis"
    var job_period = ""
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"
    logger.info("LoadHiveToRedisJob  Support parameters usage: spark-submit --class  com.aofeng.label.job.LoadHiveToRedisJob [month_id] ")
    println("LoadHiveToRedisJob  Support parameters usage: spark-submit --class  com.aofeng.label.job.LoadHiveToRedisJob [month_id] ")
    var month_id = ""
    if (args.length < 1) {
      month_id = DateUtils.getLastMonthId()
      job_period = month_id
    } else {
      month_id = args(0)
      job_period = month_id
    }
    JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)
    val target_columns: List[Column] = redis_value.split("#").toList.map(c => new Column((c)))
    try {
      val hiveDF: DataFrame = jobContext.readHiveTable(tableName).filter(s"$partition=$month_id")
        .select(org.apache.spark.sql.functions.concat_ws(",", target_columns: _*))
      //收集RDD,分批次存入redis数据库
      logger.info("LoadHiveToRedis job Prepare saveToRedis...")
      RedisUtil.saveToRedis(hiveDF, jobContext, tableName, partition, month_id, target_columns, redis_type, redis_key, redis_index.toInt, prop)
      logger.info("LoadHiveToRedis job is done! total costaTime is " + (System.currentTimeMillis() - beginTime) / 1000 + " s")
      JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
    } catch {
      case e: Exception => logger.error("LoadHiveToRedis job is failed,the message is " + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+"\t\r\n"+e.getStackTraceString)
        e.printStackTrace()
    } finally {
      jobContext.close()
    }


  }

}
