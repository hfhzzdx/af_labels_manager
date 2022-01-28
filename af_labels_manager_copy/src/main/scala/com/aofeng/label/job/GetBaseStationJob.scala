package com.aofeng.label.job


import com.alibaba.fastjson.JSON
import com.aofeng.label.constant.Constants
import com.aofeng.label.module.TableJobContext
import com.aofeng.label.reader.OutPutLabel
import com.aofeng.label.utils.{DateUtils, JdbcUtils, PropertiesUtil, StrUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer


object GetBaseStationJob {
  //  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)

    val jobContext: TableJobContext = TableJobContext.create()
    val start = System.currentTimeMillis()
    val sqlContext = jobContext.sqlContext
    //    val logger = jobContext.logger
    val properties = (new PropertiesUtil).getPropertiesByJobName("GetBaseStationJob", Constants.JOB_CONF_SERVICE_PATH)
    val base_station_user = properties.getProperty("base_station_user")
    val fields = properties.getProperty("fields")
    val labelIds = properties.getProperty("labelIds")
    val partitionFieldName = properties.getProperty("partitionFieldName")
    val sourceIdColumn = properties.getProperty("sourceIdColumn")
    val dstIdColumn = properties.getProperty("dstIdColumn")



    //读取hive表   af_res_live

    val dataTime: String = jobContext.jobConfig.loadJobConfig().getDataTime
    var af_res_liveDF: DataFrame = jobContext.sqlContext.emptyDataFrame
    val user_labels_final = jobContext.jobConfig.loadJobConfig().getHive.getTable
    val table_partition = jobContext.jobConfig.loadJobConfig().getHive.getPartitionField
    val targetIdColumn: String = jobContext.jobConfig.loadJobConfig().getHive.getIdColumn
    var month_id = ""
    //插入mysql记录表
    val job_id = jobContext.sqlContext.sparkContext.applicationId
    val job_name = "GetBaseStationJob"
    val job_desc = "河南移动精准营销2.0,获取基站对应用户信息"
    var job_period = dataTime
    val project = "hn_jzyx"
    val create_time = DateUtils.getFormatDate19Now()
    var status = "0"
    var result = "start"
    if (StringUtils.isBlank(dataTime) || StringUtils.isEmpty(dataTime)) {
      job_period = DateUtils.getLastMonthId()
    }
    JdbcUtils.insertScheduleLog(job_id, job_name, job_desc, job_period, project, create_time, status, result)


    //主程序开始

    try {
      if (StringUtils.isBlank(dataTime) || StringUtils.isEmpty(dataTime)) {
        month_id = DateUtils.getLastMonthId()
        logger.info("GetBaseStationJob read hive table is starting... month_id=" + month_id + "table name is " + properties.getProperty("stationSource"))
        val frame1 = jobContext.readHiveTable(properties.getProperty("stationSource")).filter(s"${partitionFieldName}=${month_id}")
        val frame2: DataFrame = jobContext.readHiveTable(user_labels_final).filter(s"${table_partition}=${month_id}")
        af_res_liveDF = frame1.join(frame2, frame1(sourceIdColumn) === frame2(targetIdColumn), "inner").drop(frame2.col(Constants.COLUMN_ID)).drop(frame2.col(table_partition))
      } else {
        month_id = dataTime
        logger.info("GetBaseStationJob read hive table is starting... month_id=" + month_id + "table name is " + properties.getProperty("stationSource"))
        val frame1: DataFrame = jobContext.readHiveTable(properties.getProperty("stationSource")).filter(s"${partitionFieldName}=${month_id}")
        val frame2 = jobContext.readHiveTable(user_labels_final).filter(s"${table_partition}=${month_id}")
        af_res_liveDF = frame1.join(frame2, frame1(sourceIdColumn) === frame2(targetIdColumn), "inner").drop(frame2.col(Constants.COLUMN_ID)).drop(frame2.col(table_partition))
      }

      //join完毕之后,进行dataFrame转换

      val getLabelIds = StrUtils.toList(labelIds)
      logger.info("GetBaseStationJob job is prepare to transform ...")

      val newDF: DataFrame = renameCol(jobContext, splitField(jobContext, af_res_liveDF, getLabelIds, "labels", "labelsNumber", ","), getLabelIds, "labelsNumber", "splitCols", ",")

      val base_stationDF: DataFrame = newDF.drop(sourceIdColumn).drop("labels").drop(jobContext.jobConfig.loadJobConfig().getHive.getPartitionField).drop("labels").drop("labelsNumber").drop("splitCols")

      logger.info("GetBaseStationJob job transform is done.costTime is " + (System.currentTimeMillis() - start) / 1000 + " s")
      //准备分组聚合
      val attrs = Array.tabulate(getLabelIds.length)(n => "label_" + getLabelIds(n))
      var map: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map() //初始化构造函数
      for (i <- 0 to getLabelIds.length - 1) {
        map += attrs(i) -> "sum"
      }


      logger.info("GetBaseStation job Prepare to calculate sum")
      val ouPutDF: DataFrame = base_stationDF.groupBy("lac_ci", "dn_flag").agg(map.toMap).distinct()

      logger.info("GetBaseStationJob prepare to cast to json")
      val json: RDD[String] = ouPutDF.select(ouPutDF.columns.map(c => col(c).as(c.replace("sum(", "").replace(")", ""))): _*).toJSON

      logger.info("GetBaseStationJob prepare to cast to json")
      import sqlContext.implicits._
      val finalDF: DataFrame = json.map(xJson =>
        OutPutLabel(JSON.parseObject(xJson).getString("lac_ci"),
          JSON.parseObject(xJson).getString("dn_flag"),
          xJson,
          month_id)).toDF()



      //将结果进行入HIVE操作


      logger.info("GetBaseStationJob prepare to save data to hive table...")

      jobContext.saveToHive(finalDF, base_station_user, job_period)
      logger.info("GetBaseStationJob all step is done.total costTIme is " + (System.currentTimeMillis() - start) / 1000 + " s")
      JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "1", "complete")
    } catch {
      case e: Exception => logger.error("GetBaseStationJob job is failed,the message is " + e.getMessage)
        JdbcUtils.updateScheduleLog(job_id, job_name, job_period, DateUtils.getFormatDate19Now(), "2", e.getMessage+"  \r\n"+e.getStackTraceString)
        e.printStackTrace()
    } finally {
      jobContext.close()
    }


  }

  //优化代码,编写方法
  // 方法一:切割 统计
  /**
   *
   * @param jobContext    TableJobContext
   * @param df            DataFrame
   * @param list          List collection of parent ids of labels that need to be calculated
   * @param oldColumnName Column affected by the method
   * @param newColumnName column where the method runs out
   * @param separator     separator
   * @return Returns a dataFrame containing all columns, including oldColumn and newColumn
   */
  def splitField(jobContext: TableJobContext, df: DataFrame, list: List[String], oldColumnName: String, newColumnName: String, separator: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import com.aofeng.label.utils.StrUtils
    val trans_func = udf((str: String) => {
      val str_list: List[String] = str.split(separator).toList
      import scala.collection.mutable.ListBuffer
      var numbers: ListBuffer[Int] = ListBuffer[Int]()
      var number: Int = 0
      for (label <- list) {
        if (str_list.contains(label)) {
          number = 1
        } else {
          number = 0
        }
        numbers += number
      }
      StrUtils.buffToString(numbers.toList)
    })
    df.select(col("*"), trans_func(col(oldColumnName)).as(newColumnName))
  }


  //方法二 重命名列名
  /**
   *
   * @param jobContext    TableJobContext
   * @param df            DataFrame
   * @param list          List collection of parent ids of labels that need to be calculated
   * @param oldColumnName Column affected by the method
   * @param newColumnName column where the method runs out
   * @param separator     separator
   * @return Returns a dataFrame containing all columns, including oldColumn and newColumn
   */
  def renameCol(jobContext: TableJobContext, df: DataFrame, list: List[String], oldColumnName: String, newColumnName: String, separator: String): DataFrame = {
    val attrs: Array[String] = Array.tabulate(list.length)(n => "label_" + list(n))
    import org.apache.spark.sql.functions._
    var newDF = df.withColumn(newColumnName, split(col(oldColumnName), separator))
    attrs.zipWithIndex.foreach(x => {
      newDF = newDF.withColumn(x._1, col(newColumnName).getItem(x._2))
    })
    newDF
  }


}


