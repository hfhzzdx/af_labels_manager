package com.aofeng.label.utils

import com.aofeng.label.constant.Constants
import java.io.InputStream
import java.util
import java.util.Properties

import org.apache.log4j.Logger

import scala.collection.mutable

class PropertiesUtil {
  private val logger: Logger = Logger.getLogger(this.getClass)
  val properties = new Properties()

//  def getESProperties: (String, String, String, String, String) = {
//    val path = this.getClass.getClassLoader.getResourceAsStream("src/test/resource/es.properties") //文件要放到resource文件夹下
//    properties.load(path)
//    //    println(path.toString)
//    val es_nodes: String = properties.getProperty("es.nodes")
//    //    println(es_nodes.toString)
//    val es_port: String = properties.getProperty("es.port")
//    //     println(es_port.toString)
//    val es_batch_write_retry_wait: String = properties.getProperty("es.batch.write.retry.wait")
//    val es_batch_size_entries: String = properties.getProperty("es.batch.size.entries")
//    val es_batch_size_bytes: String = properties.getProperty("es.batch.size.bytes")
//    //    val es_batch_insert_count: String = properties.getProperty("es.batch.insert.count")
//    (es_nodes, es_port, es_batch_write_retry_wait, es_batch_size_entries, es_batch_size_bytes)
//  }
//
//  def getESConfig: Properties = {
//    val path = this.getClass.getClassLoader.getResourceAsStream("src/test/resource/es.properties") //文件要放到resource文件夹下
//    properties.load(path)
//    properties
//  }

  def getGlobalConfig: Properties = {
    val path = this.getClass.getClassLoader.getResourceAsStream("global.properties") //文件要放到resource文件夹下
    properties.load(path)
    properties
  }
  def getPropertiesByJobName(jobName:String,province_job:String):Properties={
    logger.info("load PropertiesByJobName:" + jobName)
    val configPath=getGlobalConfig.getProperty(province_job,"jobConfig")
    val config: Properties = getConfig(configPath + "/" + jobName + ".properties")
    config
  }
  def getJobConfigByName(appName: String,province_job:String): Properties = {
    logger.info("load jobConfigByName:" + appName)
    val configPath = getGlobalConfig.getProperty(province_job, "jobConfig")
    val config: Properties = getConfig(configPath + "/" + appName + ".conf")
    config
  }

  def getConfig(filePath: String): Properties = {
    logger.info("load configFile:" + filePath)
    val path = this.getClass.getClassLoader.getResourceAsStream(filePath) //文件要放到resource文件夹下
    properties.load(path)
    properties
  }

  def checkFileExists(fileName:String): Boolean ={
    try {
      //文件要放到resource文件夹下
      this.getClass.getClassLoader.getResource(fileName).getPath
      true
    } catch {
      case e: NullPointerException => false
    }
  }

//  def getHiveResourceTable(): (String, String, String, String, String) = {
//    val path: InputStream = this.getClass.getClassLoader.getResourceAsStream("src/test/resource/hiveSourceTable.properties")
//    properties.load(path)
//    val dw_label_user_out_day: String = properties.getProperty("dw_label_user_out_day")
//    val phone_price: String = properties.getProperty("phone_price")
//    val dw_label_user_out_yyyymm: String = properties.getProperty("dw_label_user_out_yyyymm")
//    val dw_label_work_busi_out_yyyymm: String = properties.getProperty("dw_label_work_busi_out_yyyymm")
//    val dw_label_busi_out_yyyymm: String = properties.getProperty("dw_label_busi_out_yyyymm")
//    (dw_label_user_out_day, dw_label_user_out_yyyymm, dw_label_work_busi_out_yyyymm, dw_label_busi_out_yyyymm, phone_price)
//  }

//  def getHiveProperties(): (String, String, String) = {
//    val path: InputStream = this.getClass.getClassLoader.getResourceAsStream("src/test/resource/hiveConnection.properties")
//    properties.load(path)
//    val HIVE_URL = properties.getProperty("url")
//    val HIVE_USER_NAME = properties.getProperty("username")
//    val HIVE_PASSWD = properties.getProperty("password")
//    (HIVE_URL, HIVE_USER_NAME, HIVE_PASSWD)
//
//  }


  def propToMap(prop:Properties)={
    val sets: util.Set[AnyRef] = prop.keySet()
    import scala.collection.JavaConversions
    val propKeys: mutable.Set[AnyRef] = JavaConversions.asScalaSet(sets)
     val map=scala.collection.mutable.Map[String,String]()
   for(key<-propKeys){
       map += (key.toString->prop.getProperty(key.toString))
   }
    map
  }



}
